package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/liliang-cn/neumann/internal/neu/app"
)

// Report is what a bench run writes.
type Report struct {
	Scenario string `json:"scenario"`
	Backend  string `json:"backend"`
	Model    string `json:"model"`
	BaseURL  string `json:"base_url"`
	Cores    int    `json:"cores"`
	Route    string `json:"route,omitempty"` // hybrid: -route
	// Classes are the machine's execution groups by class: each one's
	// side (local or cloud), model and vCores. Cloud classes are billed:
	// their tokens are the run's cost.
	Classes  map[string]app.Side `json:"classes,omitempty"`
	RunID    string              `json:"run_id"`
	Work     string              `json:"work"`
	Store    string              `json:"store"`
	Started  time.Time           `json:"started"`
	Finished time.Time           `json:"finished"`
	WallS    float64             `json:"wall_s"`
	Results  []*Result           `json:"results"`
	Summary  map[string]any      `json:"summary"`
	Notes    []string            `json:"notes,omitempty"`
}

func newReport(cfg app.Config) *Report {
	r := &Report{Backend: cfg.Backend, Model: cfg.Model, BaseURL: cfg.BaseURL, Cores: cfg.Cores, Classes: map[string]app.Side{}}
	if cfg.Backend == "hybrid" {
		r.Model, r.Route = fmt.Sprintf("fast=%s strong=%s", cfg.FastModel, cfg.StrongModel), cfg.Route
		if cfg.Route == app.RouteDecide {
			r.Route += " (" + cfg.RouterModel + ")"
		}
	}
	for _, s := range cfg.Sides() {
		r.Classes[s.Class] = s
	}
	return r
}

// ClassSummary is one execution group's share of a run. Tasks, Passed and
// WallS count the tasks accepted in the class; tokens and calls count what
// ran in it (a migrated task's calls after the move count in the class it
// moved to).
type ClassSummary struct {
	Kind             string  `json:"kind"` // local or cloud
	Model            string  `json:"model"`
	Tasks            int     `json:"tasks"`
	Passed           int     `json:"passed"`
	PassRate         float64 `json:"pass_rate"`
	WallS            float64 `json:"wall_s"` // sum over its tasks
	ModelCalls       int     `json:"model_calls"`
	PromptTokens     int     `json:"prompt_tokens"`
	CachedTokens     int     `json:"cached_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	ReasoningTokens  int     `json:"reasoning_tokens"`
	MigratedIn       int     `json:"migrated_in,omitempty"`
}

// classes is the per-class accounting, and the cloud's share of it.
func (r *Report) classes() {
	if len(r.Classes) == 0 {
		return
	}
	by := map[string]*ClassSummary{}
	get := func(c string) *ClassSummary {
		if by[c] == nil {
			side := r.Classes[c]
			by[c] = &ClassSummary{Kind: side.Kind, Model: side.Model}
		}
		return by[c]
	}
	var routeMS []float64
	picks, byHow := map[string]int{}, map[string]int{}
	migrations := 0
	for _, x := range r.Results {
		if x.Class != "" {
			c := get(x.Class)
			c.Tasks++
			c.WallS += x.WallMS / 1000
			if x.Pass {
				c.Passed++
			}
		}
		for cl, u := range x.UsageByClass {
			c := get(cl)
			c.PromptTokens += u.PromptTokens
			c.CachedTokens += u.CachedTokens
			c.CompletionTokens += u.CompletionTokens
			c.ReasoningTokens += u.ReasoningTokens
		}
		for cl, n := range x.CallsByClass {
			get(cl).ModelCalls += n
		}
		if x.FinalClass != x.Class {
			get(x.FinalClass).MigratedIn++
		}
		migrations += len(x.Migrations)
		if x.Route.Class != "" {
			picks[x.Route.Class]++
			byHow[x.Route.By]++
		}
		if x.Route.By == app.RouteDecide || x.Route.By == "default" && x.Route.Took > 0 {
			routeMS = append(routeMS, x.RouteMS)
		}
	}
	var cloud ClassSummary
	for _, c := range by {
		if c.Tasks > 0 {
			c.PassRate = float64(c.Passed) / float64(c.Tasks)
		}
		if c.Kind == "cloud" {
			cloud.PromptTokens += c.PromptTokens
			cloud.CachedTokens += c.CachedTokens
			cloud.CompletionTokens += c.CompletionTokens
			cloud.ReasoningTokens += c.ReasoningTokens
		}
	}
	r.Summary["classes"] = by
	r.Summary["migrations"] = migrations
	r.Summary["cloud_prompt_tokens"] = cloud.PromptTokens
	r.Summary["cloud_cached_tokens"] = cloud.CachedTokens
	r.Summary["cloud_completion_tokens"] = cloud.CompletionTokens
	r.Summary["cloud_reasoning_tokens"] = cloud.ReasoningTokens
	if p, ok := r.Summary["passed"].(int); ok && p > 0 {
		r.Summary["cloud_tokens_per_passed_task"] = float64(cloud.PromptTokens+cloud.CompletionTokens) / float64(p)
		r.Summary["cloud_uncached_prompt_per_passed_task"] = float64(cloud.PromptTokens-cloud.CachedTokens) / float64(p)
	}
	if len(picks) > 0 {
		r.Summary["route_picks"] = picks
		r.Summary["route_by"] = byHow
	}
	if len(routeMS) > 0 {
		r.Summary["route_ms_p50"] = percentile(routeMS, 0.5)
		r.Summary["route_ms_p95"] = percentile(routeMS, 0.95)
		r.Summary["route_ms_max"] = percentile(routeMS, 1)
	}
}

func (r *Report) summarize() {
	if r.Summary == nil {
		r.Summary = map[string]any{}
	}
	defer r.classes()
	pass, done := 0, 0
	var wall, queue, model, adapter, tool, device, over []float64
	var tokP, tokC, tokR, tokCached, calls, toolCalls, retries, preempt int
	var first []float64
	for _, x := range r.Results {
		if x.Pass {
			pass++
		}
		if x.Outcome == "done" {
			done++
		}
		wall = append(wall, x.WallMS)
		queue = append(queue, x.QueueMS)
		model = append(model, x.ModelMS)
		adapter = append(adapter, x.AdapterMS)
		tool = append(tool, x.ToolMS)
		device = append(device, x.DeviceMS)
		if x.AdapterExact {
			over = append(over, x.OverheadMS)
		}
		tokP += x.Usage.PromptTokens
		tokC += x.Usage.CompletionTokens
		tokR += x.Usage.ReasoningTokens
		tokCached += x.Usage.CachedTokens
		calls += x.ModelCalls
		toolCalls += x.ToolCalls
		retries += x.Retries
		preempt += x.Preempted
		for _, f := range x.FirstMS {
			first = append(first, float64(f))
		}
	}
	r.Summary["tasks"] = len(r.Results)
	r.Summary["passed"] = pass
	r.Summary["done"] = done
	r.Summary["wall_ms_p50"] = percentile(wall, 0.5)
	r.Summary["wall_ms_max"] = percentile(wall, 1)
	r.Summary["model_calls"] = calls
	r.Summary["tool_calls"] = toolCalls
	r.Summary["retries"] = retries
	r.Summary["preemptions"] = preempt
	r.Summary["prompt_tokens"] = tokP
	r.Summary["completion_tokens"] = tokC
	r.Summary["reasoning_tokens"] = tokR
	r.Summary["cached_tokens"] = tokCached
	r.Summary["cache_hit_rate"] = hitRate(tokCached, tokP)
	r.Summary["first_ms_p50"] = percentile(first, 0.5)
	r.Summary["first_ms_p95"] = percentile(first, 0.95)
	r.Summary["sum_wall_ms"] = sum(wall)
	r.Summary["sum_queue_ms"] = sum(queue)
	r.Summary["sum_model_ms"] = sum(model)
	r.Summary["sum_adapter_ms"] = sum(adapter)
	r.Summary["sum_tool_ms"] = sum(tool)
	r.Summary["sum_device_ms"] = sum(device)
	r.Summary["sum_overhead_ms"] = sum(over)
	if n := calls; n > 0 {
		r.Summary["overhead_ms_per_model_call"] = sum(over) / float64(n)
	}
}

func sum(xs []float64) float64 {
	t := 0.0
	for _, x := range xs {
		t += x
	}
	return t
}

// Write saves the report as indented JSON.
func (r *Report) Write(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func secs(ms float64) string { return fmt.Sprintf("%.1f", ms/1000) }

// Print writes a compact table of the results and the summary.
func (r *Report) Print(w io.Writer) {
	route := ""
	if r.Route != "" {
		route = ", route " + r.Route
	}
	fmt.Fprintf(w, "\n== %s on %s (%s), %d vCores%s, %.0fs ==\n", r.Scenario, r.Backend, r.Model, r.Cores, route, r.WallS)
	tw := tabwriter.NewWriter(w, 0, 0, 1, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "task\tclass\tok\tend\twall s\tqueue s\tmodel s\tadapter s\ttool s\tdevice s\toverhead ms\tcalls\ttools\tprompt\tcached\tcompl\treason\tfirst ms\tretry\tpreempt\t")
	for _, x := range r.Results {
		ok := "FAIL"
		if x.Pass {
			ok = "pass"
		}
		over := "-"
		if x.AdapterExact {
			over = fmt.Sprintf("%.0f", x.OverheadMS)
		}
		firsts := make([]string, 0, len(x.FirstMS))
		for _, f := range x.FirstMS {
			firsts = append(firsts, fmt.Sprint(f))
		}
		fs := strings.Join(firsts, ",")
		if len(fs) > 24 {
			fs = fs[:23] + "…"
		}
		name := x.ID
		if i := strings.Index(name, "."); i >= 0 {
			name = name[i+1:]
		}
		class := x.Class
		if x.FinalClass != x.Class {
			class += "→" + x.FinalClass
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%s\t%d\t%d\t\n",
			name, class, ok, x.Outcome, secs(x.WallMS), secs(x.QueueMS), secs(x.ModelMS), secs(x.AdapterMS), secs(x.ToolMS), secs(x.DeviceMS), over,
			x.ModelCalls, x.ToolCalls, x.Usage.PromptTokens, fmt.Sprintf("%d (%.0f%%)", x.Usage.CachedTokens, x.CacheHitRate), x.Usage.CompletionTokens, x.Usage.ReasoningTokens, fs, x.Retries, x.Preempted)
	}
	tw.Flush()
	var fails []string
	for _, x := range r.Results {
		if !x.Pass {
			fails = append(fails, fmt.Sprintf("  %s: %s", x.ID, x.Detail))
		}
	}
	if len(fails) > 0 {
		fmt.Fprintln(w, "failures:")
		fmt.Fprintln(w, strings.Join(fails, "\n"))
	}
	keys := make([]string, 0, len(r.Summary))
	for k := range r.Summary {
		if k == "status_samples" || k == "probes" || k == "classes" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintln(w, "summary:")
	for _, k := range keys {
		v := r.Summary[k]
		if f, ok := v.(float64); ok {
			v = fmt.Sprintf("%.1f", f)
		}
		fmt.Fprintf(w, "  %-38s %v\n", k, v)
	}
	if cs, ok := r.Summary["classes"].(map[string]*ClassSummary); ok {
		names := make([]string, 0, len(cs))
		for n := range cs {
			names = append(names, n)
		}
		sort.Strings(names)
		fmt.Fprintln(w, "per class:")
		ctw := tabwriter.NewWriter(w, 0, 0, 1, ' ', tabwriter.AlignRight)
		fmt.Fprintln(ctw, "class\tside\tmodel\ttasks\tpassed\twall s\tcalls\tprompt\tcached\tcompl\treason\tmigrated in\t")
		for _, n := range names {
			c := cs[n]
			fmt.Fprintf(ctw, "%s\t%s\t%s\t%d\t%d\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t\n", n, c.Kind, c.Model, c.Tasks, c.Passed, c.WallS, c.ModelCalls, c.PromptTokens, c.CachedTokens, c.CompletionTokens, c.ReasoningTokens, c.MigratedIn)
		}
		ctw.Flush()
	}
	if p, ok := r.Summary["probes"]; ok {
		b, _ := json.Marshal(p)
		fmt.Fprintf(w, "  probes %s\n", b)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "note: %s\n", n)
	}
}
