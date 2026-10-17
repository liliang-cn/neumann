package bench

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
)

// Report is what a bench run writes.
type Report struct {
	Scenario string         `json:"scenario"`
	Backend  string         `json:"backend"`
	Model    string         `json:"model"`
	BaseURL  string         `json:"base_url"`
	Cores    int            `json:"cores"`
	RunID    string         `json:"run_id"`
	Work     string         `json:"work"`
	Store    string         `json:"store"`
	Started  time.Time      `json:"started"`
	Finished time.Time      `json:"finished"`
	WallS    float64        `json:"wall_s"`
	Results  []*Result      `json:"results"`
	Summary  map[string]any `json:"summary"`
	Notes    []string       `json:"notes,omitempty"`
}

func (r *Report) summarize() {
	if r.Summary == nil {
		r.Summary = map[string]any{}
	}
	pass, done := 0, 0
	var wall, queue, model, adapter, tool, device, over []float64
	var tokP, tokC, tokR, calls, toolCalls, retries, preempt int
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
	fmt.Fprintf(w, "\n== %s on %s (%s), %d vCores, %.0fs ==\n", r.Scenario, r.Backend, r.Model, r.Cores, r.WallS)
	tw := tabwriter.NewWriter(w, 0, 0, 1, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "task\tok\tend\twall s\tqueue s\tmodel s\tadapter s\ttool s\tdevice s\toverhead ms\tcalls\ttools\tprompt\tcompl\treason\tfirst ms\tretry\tpreempt\t")
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
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t\n",
			name, ok, x.Outcome, secs(x.WallMS), secs(x.QueueMS), secs(x.ModelMS), secs(x.AdapterMS), secs(x.ToolMS), secs(x.DeviceMS), over,
			x.ModelCalls, x.ToolCalls, x.Usage.PromptTokens, x.Usage.CompletionTokens, x.Usage.ReasoningTokens, fs, x.Retries, x.Preempted)
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
		if k == "status_samples" || k == "probes" {
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
		fmt.Fprintf(w, "  %-28s %v\n", k, v)
	}
	if p, ok := r.Summary["probes"]; ok {
		b, _ := json.Marshal(p)
		fmt.Fprintf(w, "  probes %s\n", b)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "note: %s\n", n)
	}
}
