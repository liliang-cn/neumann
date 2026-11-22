// Command cortex attaches CortexDB to a machine as two bus devices, a reader
// and a writer, and shows that a memory written just before a crash is not
// written twice when the call is sent again after the restart.
//
//	go build -o /tmp/cortexdb-mcp-stdio ./cmd/cortexdb-mcp-stdio   # in the CortexDB repo
//	go run ./examples/cortex -server /tmp/cortexdb-mcp-stdio
//
// The server is CortexDB's own MCP server over stdio (-server, or
// CORTEXDB_MCP_BIN), on a fresh SQLite file (-db, default a temporary
// directory). It always runs in local mode: CORTEXDB_REMOTE and
// CORTEXDB_GRPC_TOKEN are cleared for it, so the example never touches a
// shared brain. The model is scripted (sim).
//
// # The devices
//
// CortexDB serves about 130 tools and annotates none of them, so through
// bus/mcp every one is SideEffect. The example wraps each MCP device in a
// filter that keeps a few tools and declares their effect:
//
//   - cortex: knowledge_memory_recall, memory_search and memory_get,
//     ReadOnly and Cacheable. Recall bumps a memory's recall count, which is
//     harmless to repeat.
//   - cortexw: memory_save, Idempotent, one call at a time. Config.MaxPerDevice
//     is one number for every device, so the writer serializes itself.
//
// Delete, purge and the rest are left out; a host that wants them keeps them
// SideEffect, so a crash mid-call is held for an operator.
//
// # Why memory_save is Idempotent
//
// memory_save is an upsert on memory_id. The writer sets memory_id to
// neu:<task>:<call> from the call's idempotency key (task/call/attempt),
// dropping the attempt: a call sent again after a restart, or as a later
// attempt, writes the same row. The model's own memory_id, if it gives one,
// is replaced; replacing an old memory is done with supersedes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/mcp"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// The tools each device keeps, with their effect.
var (
	readTools = map[string]bus.Effect{
		"knowledge_memory_recall": bus.ReadOnly,
		"memory_search":           bus.ReadOnly,
		"memory_get":              bus.ReadOnly,
	}
	writeTools = map[string]bus.Effect{
		"memory_save": bus.Idempotent,
	}
)

const fact = "The user wants every interface light by default; dark is only a second option."

func main() {
	server := flag.String("server", os.Getenv("CORTEXDB_MCP_BIN"), "CortexDB MCP server binary (cortexdb-mcp-stdio)")
	dbPath := flag.String("db", "", "CortexDB file (default: a fresh one in a temporary directory)")
	flag.Parse()
	if *server == "" {
		if p, err := exec.LookPath("cortexdb-mcp-stdio"); err == nil {
			*server = p
		} else {
			log.Fatal("no CortexDB MCP server: pass -server or set CORTEXDB_MCP_BIN (build it with go build ./cmd/cortexdb-mcp-stdio in the CortexDB repo)")
		}
	}
	dir, err := os.MkdirTemp("", "neumann-cortex-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if *dbPath == "" {
		*dbPath = filepath.Join(dir, "cortex.db")
	}
	statePath := filepath.Join(dir, "neumann.db")
	ctx := context.Background()

	// Run 1: the write reaches CortexDB, then the process dies before the
	// machine records the result.
	crashed := make(chan struct{})
	var once sync.Once
	s1, err := sqlite.Open(statePath)
	if err != nil {
		log.Fatal(err)
	}
	ds := &dyingStore{Store: s1}
	m1 := machine(ds, *server, *dbPath, func() {
		ds.die()
		once.Do(func() { close(crashed) })
	})
	if err := m1.Start(ctx); err != nil {
		log.Fatal(err)
	}
	id, err := m1.Submit(ctx, proto.Instruction{Kind: proto.KindSubmit, TaskID: "remember-1", Input: "Remember that I want light interfaces by default."})
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-crashed:
	case <-time.After(time.Minute):
		log.Fatal("run 1: memory_save never ran")
	}
	fmt.Println("run 1: memory_save reached CortexDB; the process dies before its result is committed")
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_ = m1.Stop(stopCtx) // commits fail from here on, as in a dead process
	cancel()
	s1.Close()

	// Run 2: the same state file; recovery sends the dispatched Idempotent
	// call again under the same key.
	s2, err := sqlite.Open(statePath)
	if err != nil {
		log.Fatal(err)
	}
	defer s2.Close()
	for _, r := range mustCalls(s2.Calls(ctx, id)) {
		fmt.Printf("run 2 finds: %-18s %-10s effect=%s key=%s\n", r.Tool, r.Status, r.Effect, r.Key)
	}
	m2 := machine(s2, *server, *dbPath, nil)
	ectx, ecancel := context.WithTimeout(ctx, 2*time.Minute)
	defer ecancel()
	events := m2.Events(ectx, 0)
	if err := m2.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer m2.Stop(ctx)
	for ev := range events {
		if ev.TaskID != id {
			continue
		}
		switch ev.Kind {
		case proto.EvToolCall, proto.EvToolResult, proto.EvRecovered, proto.EvDone:
			fmt.Printf("run 2: %-12s %s\n", ev.Kind, clip(string(ev.Data), 160))
		}
		if ev.Terminal() {
			break
		}
	}
	t, err := m2.Load(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%s: %s\n", t.State, t.LastAnswer())

	n, err := countSaved(ctx, *server, *dbPath, "neu:"+id+":")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("memories this task wrote: %d\n", n)
	if n != 1 {
		log.Fatalf("want exactly 1 memory after the replay, got %d", n)
	}
}

// machine builds a machine over store with the two CortexDB devices.
// afterWrite, when set, runs after memory_save has reached CortexDB and
// before its result goes back to the machine.
func machine(store mem.Store, server, dbPath string, afterWrite func()) *neumann.Machine {
	read := &cortexDevice{name: "cortex", inner: stdio("cortex", server, dbPath, readTools), keep: readTools, cache: true}
	write := &cortexDevice{name: "cortexw", inner: stdio("cortexw", server, dbPath, writeTools), keep: writeTools, serial: true, after: afterWrite}
	m, err := neumann.New(neumann.Config{
		Model:   script(),
		Store:   store,
		Devices: []bus.Device{read, write},
		System:  "You keep the user's long-term memory in CortexDB. Answer in one sentence.",
		Logf:    func(string, ...any) {}, // run 1 logs every failed commit; the output is the point here
	})
	if err != nil {
		log.Fatal(err)
	}
	return m
}

// stdio is CortexDB's MCP server on dbPath, forced to local mode, with the
// effects the example declares for the tools it keeps.
func stdio(name, server, dbPath string, keep map[string]bus.Effect) *mcp.Device {
	opts := []mcp.Option{
		mcp.WithEnv("CORTEXDB_REMOTE=", "CORTEXDB_GRPC_TOKEN=", "CORTEXDB_PATH="+dbPath),
		mcp.WithStderr(os.Stderr),
	}
	for tool, e := range keep {
		opts = append(opts, mcp.WithEffect(tool, e))
	}
	return mcp.Stdio(name, server, nil, opts...)
}

// cortexDevice is a CortexDB MCP device cut down to the tools in keep. A
// serial device runs one call at a time. memory_save gets a memory_id derived
// from the call's idempotency key (see memoryID).
type cortexDevice struct {
	name   string
	inner  bus.Device
	keep   map[string]bus.Effect
	cache  bool   // ReadOnly tools may be served from L2
	serial bool   // one call at a time
	after  func() // test hook: after a memory_save reached the server
	mu     sync.Mutex
}

func (d *cortexDevice) Name() string { return d.name }
func (d *cortexDevice) Close() error { return d.inner.Close() }

func (d *cortexDevice) Tools(ctx context.Context) ([]bus.ToolDef, error) {
	defs, err := d.inner.Tools(ctx)
	if err != nil {
		return nil, err
	}
	var out []bus.ToolDef
	for _, def := range defs {
		e, ok := d.keep[def.Name]
		if !ok {
			continue
		}
		def.Effect = e
		def.Cacheable = d.cache && e == bus.ReadOnly
		out = append(out, def)
	}
	if len(out) != len(d.keep) {
		return nil, fmt.Errorf("%s: the server has %d of the %d tools this device keeps", d.name, len(out), len(d.keep))
	}
	return out, nil
}

func (d *cortexDevice) Call(ctx context.Context, call bus.DeviceCall) (task.ToolResult, error) {
	if _, ok := d.keep[call.Tool]; !ok {
		return task.ToolResult{}, fmt.Errorf("%s has no tool %q", d.name, call.Tool)
	}
	if d.serial {
		d.mu.Lock()
		defer d.mu.Unlock()
	}
	if call.Tool == "memory_save" {
		id, err := memoryID(call)
		if err != nil {
			return task.ToolResult{}, err
		}
		if call.Args, err = withField(call.Args, "memory_id", id); err != nil {
			return task.ToolResult{}, err
		}
	}
	res, err := d.inner.Call(ctx, call)
	if call.Tool == "memory_save" && err == nil && !res.IsError && d.after != nil {
		d.after()
	}
	return res, err
}

// memoryID is neu:<task>:<call> from a call's key, task/call/attempt: the
// same for every attempt of one call, so a repeat overwrites its own row.
func memoryID(call bus.DeviceCall) (string, error) {
	rest, ok := strings.CutPrefix(call.Key, call.Task+"/")
	if call.Task == "" || !ok {
		return "", fmt.Errorf("memory_save needs the call's idempotency key, got %q", call.Key)
	}
	i := strings.LastIndexByte(rest, '/')
	if i <= 0 {
		return "", fmt.Errorf("memory_save: key %q is not task/call/attempt", call.Key)
	}
	return "neu:" + call.Task + ":" + rest[:i], nil
}

// withField sets one field of a JSON object.
func withField(args json.RawMessage, key, value string) (json.RawMessage, error) {
	obj := map[string]json.RawMessage{}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &obj); err != nil || obj == nil {
			return nil, errors.New("arguments must be a JSON object")
		}
	}
	v, _ := json.Marshal(value)
	obj[key] = v
	return json.Marshal(obj)
}

// dyingStore is a process that dies on demand: from die on, every commit
// fails, so whatever happened in the world after that is never recorded.
type dyingStore struct {
	mem.Store
	dead atomic.Bool
}

var errDead = errors.New("process died")

func (s *dyingStore) die() { s.dead.Store(true) }

func (s *dyingStore) Commit(ctx context.Context, cs mem.CommitSet) ([]proto.Event, error) {
	if s.dead.Load() {
		return nil, errDead
	}
	return s.Store.Commit(ctx, cs)
}

// script saves the fact, recalls it, and answers with what recall found.
func script() model.Adapter {
	call := func(tool, args string) model.Response {
		return model.Response{FinishReason: "tool_calls", Message: task.Message{Role: task.RoleAssistant, Parts: []task.Part{
			{Kind: task.PartToolCall, ProviderCallID: "p-" + tool, Name: tool, Args: json.RawMessage(args)},
		}}}
	}
	return &sim.Model{Fn: func(req model.Request) (model.Response, error) {
		switch sim.ToolRounds(req) {
		case 0:
			args, _ := json.Marshal(map[string]any{"content": fact, "importance": 0.9})
			return call("cortexw_memory_save", string(args)), nil
		case 1:
			return call("cortex_knowledge_memory_recall", `{"query":"light interface default","disable_knowledge":true}`), nil
		}
		last := req.Messages[len(req.Messages)-1]
		for _, p := range last.Parts {
			if p.Kind == task.PartToolResult && strings.Contains(p.Text, "light by default") {
				return sim.Reply("Saved, and recall finds it: interfaces are light by default."), nil
			}
		}
		return sim.Reply("Saved, but recall did not find it."), nil
	}}
}

// countSaved counts the memories whose id starts with prefix, straight from
// the server, outside any machine.
func countSaved(ctx context.Context, server, dbPath, prefix string) (int, error) {
	d := stdio("count", server, dbPath, nil)
	defer d.Close()
	res, err := d.Call(ctx, bus.DeviceCall{Tool: "memory_list_all", Args: json.RawMessage(`{}`)})
	if err != nil || res.IsError {
		return 0, fmt.Errorf("memory_list_all: %v %s", err, res.Content)
	}
	var out struct {
		Memories []struct {
			ID string `json:"id"`
		} `json:"memories"`
	}
	if err := json.Unmarshal([]byte(res.Content), &out); err != nil {
		return 0, err
	}
	n := 0
	for _, m := range out.Memories {
		if strings.HasPrefix(m.ID, prefix) {
			n++
		}
	}
	return n, nil
}

func mustCalls(rs []mem.CallRecord, err error) []mem.CallRecord {
	if err != nil {
		log.Fatal(err)
	}
	return rs
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
