package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/task"
)

// fake is a server with more tools than any cortexDevice keeps, all
// SideEffect, recording the arguments memory_save receives.
type fake struct {
	mu      sync.Mutex
	saved   []string // memory_id of each memory_save
	running atomic.Int32
	most    atomic.Int32
	old     bool // a server before v2.166.0: no annotations, every tool SideEffect
}

// declared is what CortexDB v2.166.0 declares for the fake's tools.
var declared = map[string]bus.Effect{
	"memory_save": bus.Idempotent, "memory_delete": bus.Idempotent,
	"knowledge_memory_recall": bus.ReadOnly, "memory_search": bus.ReadOnly,
	"memory_get": bus.ReadOnly, "graph_cypher_query": bus.ReadOnly,
}

func (f *fake) Name() string { return "fake" }
func (f *fake) Close() error { return nil }

func (f *fake) Tools(context.Context) ([]bus.ToolDef, error) {
	var defs []bus.ToolDef
	for _, n := range []string{"memory_save", "memory_delete", "knowledge_memory_recall", "memory_search", "memory_get", "graph_cypher_query"} {
		def := bus.ToolDef{Name: n}
		if !f.old {
			def.Effect = declared[n]
		}
		defs = append(defs, def)
	}
	return defs, nil
}

func (f *fake) Call(_ context.Context, c bus.DeviceCall) (task.ToolResult, error) {
	n := f.running.Add(1)
	defer f.running.Add(-1)
	for {
		m := f.most.Load()
		if n <= m || f.most.CompareAndSwap(m, n) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	if c.Tool == "memory_save" {
		var a struct {
			MemoryID string `json:"memory_id"`
		}
		json.Unmarshal(c.Args, &a)
		f.mu.Lock()
		f.saved = append(f.saved, a.MemoryID)
		f.mu.Unlock()
	}
	return task.ToolResult{Content: "ok"}, nil
}

func TestToolsAreFilteredAndDeclared(t *testing.T) {
	ctx := context.Background()
	r := &cortexDevice{name: "cortex", inner: &fake{}, keep: readTools, cache: true}
	defs, err := r.Tools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != len(readTools) {
		t.Fatalf("reader serves %d tools, want %d", len(defs), len(readTools))
	}
	for _, d := range defs {
		if d.Effect != bus.ReadOnly || !d.Cacheable {
			t.Errorf("%s: effect %s cacheable %v, want read_only and cacheable", d.Name, d.Effect, d.Cacheable)
		}
	}
	w := &cortexDevice{name: "cortexw", inner: &fake{}, keep: writeTools, serial: true}
	defs, err = w.Tools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Name != "memory_save" || defs[0].Effect != bus.Idempotent || defs[0].Cacheable {
		t.Fatalf("writer serves %+v, want memory_save, idempotent, not cacheable", defs)
	}
	if _, err := w.Call(ctx, bus.DeviceCall{Tool: "memory_delete", Key: "t/c/1", Task: "t"}); err == nil {
		t.Fatal("writer ran memory_delete, which it does not keep")
	}
	old := &cortexDevice{name: "cortexw", inner: &fake{old: true}, keep: writeTools}
	if _, err := old.Tools(ctx); err == nil {
		t.Fatal("a server that declares memory_save SideEffect attached as Idempotent")
	}
	missing := &cortexDevice{name: "x", inner: &fake{}, keep: map[string]bus.Effect{"no_such_tool": bus.ReadOnly}}
	if _, err := missing.Tools(ctx); err == nil {
		t.Fatal("a device keeping a tool the server lacks attached anyway")
	}
}

// Every attempt of one call writes the same row, whatever memory_id the
// model gave; another call writes another.
func TestMemoryIDIsOnePerCall(t *testing.T) {
	ctx := context.Background()
	f := &fake{}
	w := &cortexDevice{name: "cortexw", inner: f, keep: writeTools, serial: true}
	for _, c := range []bus.DeviceCall{
		{Tool: "memory_save", Args: json.RawMessage(`{"content":"x"}`), Key: "a/b/c-1/1", Task: "a/b"},
		{Tool: "memory_save", Args: json.RawMessage(`{"content":"x","memory_id":"mine"}`), Key: "a/b/c-1/2", Task: "a/b"},
		{Tool: "memory_save", Args: json.RawMessage(`{"content":"y"}`), Key: "a/b/c-2/1", Task: "a/b"},
	} {
		if _, err := w.Call(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"neu:a/b:c-1", "neu:a/b:c-1", "neu:a/b:c-2"}
	for i := range want {
		if f.saved[i] != want[i] {
			t.Fatalf("memory ids %q, want %q", f.saved, want)
		}
	}
	for _, c := range []bus.DeviceCall{
		{Tool: "memory_save", Args: json.RawMessage(`{"content":"x"}`)},
		{Tool: "memory_save", Args: json.RawMessage(`{"content":"x"}`), Key: "t/c", Task: "t"},
		{Tool: "memory_save", Args: json.RawMessage(`{"content":"x"}`), Key: "u/c/1", Task: "t"},
		{Tool: "memory_save", Args: json.RawMessage(`["x"]`), Key: "t/c/1", Task: "t"},
	} {
		if _, err := w.Call(ctx, c); err == nil {
			t.Errorf("key %q args %s: saved, want an error", c.Key, c.Args)
		}
	}
}

func TestWriterRunsOneCallAtATime(t *testing.T) {
	f := &fake{}
	w := &cortexDevice{name: "cortexw", inner: f, keep: writeTools, serial: true}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "t/c-" + string(rune('0'+i)) + "/1"
			w.Call(context.Background(), bus.DeviceCall{Tool: "memory_save", Args: json.RawMessage(`{"content":"x"}`), Key: key, Task: "t"})
		}()
	}
	wg.Wait()
	if m := f.most.Load(); m != 1 {
		t.Fatalf("writer ran %d calls at once, want 1", m)
	}
}
