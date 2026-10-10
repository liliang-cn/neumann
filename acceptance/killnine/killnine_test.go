// Package killnine is design §19 acceptance 2 against a real model: a
// multi-round task whose second round calls a SideEffect tool is killed with
// SIGKILL while that tool runs. The restarted machine holds the call in
// Waiting(Reconcile) instead of sending it again; once an operator resolves
// it from the ledger, the task completes, and the ledger shows the payment
// exactly once.
//
// The machine runs as a child process behind the stdio frontend
// (proto/jsonl), the same server cmd/neumann runs, with a "bank" device
// added: the test binary re-executes itself as that child. The parent drives
// it over stdin and stdout only, kills it with SIGKILL, and reads the SQLite
// file between processes.
//
// Skipped unless NEUMANN_ACCEPT_LIVE=1. The endpoint comes from the
// environment, OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_MODEL or the LLM_
// equivalents; the key is passed on to the child by environment only.
//
//	NEUMANN_ACCEPT_LIVE=1 go test -v -count=3 -timeout 30m ./acceptance/killnine
//
// NEUMANN_KILLNINE_DIR=path keeps each run's database and ledger under path.
package killnine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/proto/jsonl"
	"github.com/liliang-cn/neumann/task"
)

const (
	childEnv  = "NEUMANN_KILLNINE_CHILD" // set: this process is the machine
	dbEnv     = "NEUMANN_KILLNINE_DB"
	ledgerEnv = "NEUMANN_KILLNINE_LEDGER"
	hangEnv   = "NEUMANN_KILLNINE_HANG" // "1": a payment blocks until the process dies

	invoice = "INV-7731"
	payee   = "ACME Industrial Supplies GmbH"
	amount  = "1284.50"
	// ref is printed only by the lookup, and pay refuses any other: the model
	// cannot pay in the first round, before it has looked the invoice up.
	ref = "PR-55120-QX"

	prompt = "Please settle invoice " + invoice + " for me. First look it up with the lookup tool to get the payee, " +
		"the amount and its payment reference. Then pay it exactly once with the pay tool, using those exact details. " +
		"Finally tell me, in one sentence, who was paid, how much, and the transaction id."
)

func env(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" {
		os.Exit(child())
	}
	os.Exit(m.Run())
}

// tx is the transaction id a payment under key gets: the operator finds it
// in the ledger and hands it back when resolving.
func tx(key string) string {
	h := sha256.Sum256([]byte(key))
	return "TX-" + strings.ToUpper(hex.EncodeToString(h[:4]))
}

// entry is one ledger line: one payment that really happened.
type entry struct {
	Key  string          `json:"key"`
	TX   string          `json:"tx"`
	Args json.RawMessage `json:"args"`
	PID  int             `json:"pid"`
}

// bank is the outside world: lookup is ReadOnly, pay is a SideEffect that
// appends to the ledger file, which no process crash can take back.
func bank(ledger string, hang bool) bus.Device {
	lookupSchema := json.RawMessage(`{"type":"object","properties":{"invoice":{"type":"string","description":"invoice number, e.g. INV-1234"}},"required":["invoice"]}`)
	paySchema := json.RawMessage(`{"type":"object","properties":{
		"payee":{"type":"string"},"amount":{"type":"string","description":"decimal amount, e.g. 10.00"},
		"currency":{"type":"string"},"reference":{"type":"string","description":"the invoice's payment reference"}},
		"required":["payee","amount","currency","reference"]}`)
	return builtin.New("bank").
		Add(bus.ToolDef{Name: "lookup", Description: "Look up an invoice by number: payee, amount, currency and payment reference.", Schema: lookupSchema, Effect: bus.ReadOnly},
			func(_ context.Context, c bus.DeviceCall) (string, error) {
				var v struct{ Invoice string }
				if err := json.Unmarshal(c.Args, &v); err != nil {
					return "", fmt.Errorf("bad arguments: %v", err)
				}
				if !strings.EqualFold(strings.TrimSpace(v.Invoice), invoice) {
					return "", fmt.Errorf("no invoice %q", v.Invoice)
				}
				return fmt.Sprintf("Invoice %s: payee %s, amount %s EUR, payment reference %s, status open.", invoice, payee, amount, ref), nil
			}).
		Add(bus.ToolDef{Name: "pay", Description: "Transfer money to a payee. This moves real money: call it once per invoice.", Schema: paySchema, Effect: bus.SideEffect, Timeout: 30 * time.Minute},
			func(ctx context.Context, c bus.DeviceCall) (string, error) {
				var v struct{ Payee, Amount, Currency, Reference string }
				if err := json.Unmarshal(c.Args, &v); err != nil {
					return "", fmt.Errorf("bad arguments: %v", err)
				}
				if strings.TrimSpace(v.Reference) != ref {
					return "", fmt.Errorf("rejected: unknown payment reference %q", v.Reference)
				}
				line, _ := json.Marshal(entry{Key: c.Key, TX: tx(c.Key), Args: c.Args, PID: os.Getpid()})
				f, err := os.OpenFile(ledger, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
				if err != nil {
					return "", err
				}
				f.Write(append(line, '\n'))
				f.Sync()
				f.Close()
				if hang {
					<-ctx.Done() // the process is killed while the payment is in flight
					return "", ctx.Err()
				}
				return fmt.Sprintf("Paid %s %s to %s, transaction id %s.", v.Amount, v.Currency, v.Payee, tx(c.Key)), nil
			})
}

// child is the machine process: cmd/neumann's stdio server, with the bank
// device attached and the store and ledger the parent names.
func child() int {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	base, key, name := env("OPENAI_BASE_URL", "LLM_BASE_URL"), env("OPENAI_API_KEY", "LLM_API_KEY"), env("OPENAI_MODEL", "LLM_MODEL")
	s, err := sqlite.Open(os.Getenv(dbEnv))
	if err != nil {
		logger.Print(err)
		return 1
	}
	defer s.Close()
	m, err := neumann.New(neumann.Config{
		Model:   openai.New(base, key, name),
		Store:   s,
		Devices: []bus.Device{bank(os.Getenv(ledgerEnv), os.Getenv(hangEnv) == "1")},
		Logf:    logger.Printf,
	})
	if err != nil {
		logger.Print(err)
		return 1
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		logger.Printf("start: %v", err)
		return 1
	}
	logger.Printf("child %d ready: model %s", os.Getpid(), name)
	if err := (&jsonl.Server{Machine: m, Logf: logger.Printf}).Serve(ctx, os.Stdin, os.Stdout); err != nil {
		logger.Print(err)
		return 1
	}
	return 0
}

// proc is one machine process driven over its stdio.
type proc struct {
	t      *testing.T
	name   string
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan map[string]any
	stderr *syncBuffer
	done   chan struct{}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func launch(t *testing.T, name, db, ledger string, hang bool) *proc {
	t.Helper()
	h := "0"
	if hang {
		h = "1"
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), childEnv+"=1", dbEnv+"="+db, ledgerEnv+"="+ledger, hangEnv+"="+h)
	p := &proc{t: t, name: name, cmd: cmd, lines: make(chan map[string]any, 4096), stderr: &syncBuffer{}, done: make(chan struct{})}
	cmd.Stderr = p.stderr
	var err error
	if p.stdin, err = cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(p.done)
		defer close(p.lines)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(nil, 1<<24)
		for sc.Scan() {
			var v map[string]any
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
				t.Errorf("%s: stdout line is not JSON: %s", name, sc.Bytes())
				continue
			}
			p.lines <- v
		}
		cmd.Wait()
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		<-p.done
		if k := env("OPENAI_API_KEY", "LLM_API_KEY"); k != "" && strings.Contains(p.stderr.String(), k) {
			t.Errorf("%s: the API key appears on stderr", name)
		}
	})
	return p
}

func (p *proc) send(v map[string]any) {
	p.t.Helper()
	b, _ := json.Marshal(v)
	if _, err := p.stdin.Write(append(b, '\n')); err != nil {
		p.t.Fatalf("%s: write %s: %v", p.name, b, err)
	}
}

// until reads lines, logging events, until stop says so; it fails after d.
func (p *proc) until(d time.Duration, stop func(map[string]any) bool) map[string]any {
	p.t.Helper()
	timeout := time.After(d)
	for {
		select {
		case v, ok := <-p.lines:
			if !ok {
				p.t.Fatalf("%s: stdout ended; stderr:\n%s", p.name, p.stderr.String())
			}
			if v["type"] == "event" {
				p.t.Logf("[%s] %s", p.name, show(v))
			} else if v["ok"] != true {
				p.t.Fatalf("%s: refused: %v", p.name, v)
			}
			if stop(v) {
				return v
			}
		case <-timeout:
			p.t.Fatalf("%s: nothing after %v; stderr:\n%s", p.name, d, p.stderr.String())
		}
	}
}

// close ends stdin, so the server stops the machine gracefully, and waits.
func (p *proc) close() {
	p.t.Helper()
	p.stdin.Close()
	go func() {
		for range p.lines {
		}
	}()
	select {
	case <-p.done:
	case <-time.After(60 * time.Second):
		p.t.Fatalf("%s did not exit; stderr:\n%s", p.name, p.stderr.String())
	}
	if code := p.cmd.ProcessState.ExitCode(); code != 0 {
		p.t.Fatalf("%s exited %d; stderr:\n%s", p.name, code, p.stderr.String())
	}
}

func show(v map[string]any) string {
	data, _ := json.Marshal(v["data"])
	s := string(data)
	if len(s) > 240 {
		s = s[:240] + "…"
	}
	return fmt.Sprintf("seq=%v task_seq=%v %s %s", v["seq"], v["task_seq"], v["kind"], s)
}

func ledgerEntries(t *testing.T, path string) []entry {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []entry
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l == "" {
			continue
		}
		var e entry
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			t.Fatalf("ledger line %q: %v", l, err)
		}
		out = append(out, e)
	}
	return out
}

// snapshot opens the store between processes and reports the task and its
// call records; it returns them and the highest event seq so far.
func snapshot(t *testing.T, db, id, when string) (*task.TCB, []mem.CallRecord, int64) {
	t.Helper()
	s, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	tcb, err := s.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	calls, err := s.Calls(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	evs, err := s.Events(ctx, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var last int64
	if len(evs) > 0 {
		last = evs[len(evs)-1].Seq
	}
	t.Logf("--- store %s: task %s state=%s wait=%s stop=%q steps=%d unconfirmed=%d version=%d, last seq %d",
		when, id, tcb.State, tcb.Wait, tcb.StopReason, tcb.Used.Steps, tcb.Unconfirmed, tcb.Version, last)
	for _, c := range calls {
		res := "<none>"
		if c.Result != nil {
			res = fmt.Sprintf("%q is_error=%v", c.Result.Content, c.Result.IsError)
		}
		t.Logf("    call %s #%d %s effect=%s status=%s attempt=%d key=%s args=%s result=%s",
			c.CallID, c.Index, c.Tool, c.Effect, c.Status, c.Attempt, c.Key, c.Args, res)
	}
	return tcb, calls, last
}

func TestKillNineLive(t *testing.T) {
	if os.Getenv("NEUMANN_ACCEPT_LIVE") != "1" {
		t.Skip("live acceptance against a real model: set NEUMANN_ACCEPT_LIVE=1 and the endpoint variables")
	}
	if env("OPENAI_BASE_URL", "LLM_BASE_URL") == "" || env("OPENAI_API_KEY", "LLM_API_KEY") == "" || env("OPENAI_MODEL", "LLM_MODEL") == "" {
		t.Fatal("set OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL (or the LLM_ equivalents)")
	}
	dir := t.TempDir()
	if keep := os.Getenv("NEUMANN_KILLNINE_DIR"); keep != "" {
		dir = filepath.Join(keep, time.Now().Format("20060102-150405.000"))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	db, ledger := filepath.Join(dir, "neumann.db"), filepath.Join(dir, "ledger.jsonl")
	id := fmt.Sprintf("killnine-%d", time.Now().UnixNano())
	t.Logf("model %s, dir %s, task %s", env("OPENAI_MODEL", "LLM_MODEL"), dir, id)
	isTask := func(v map[string]any) bool { return v["type"] == "event" && v["task_id"] == id }
	data := func(v map[string]any) map[string]any { d, _ := v["data"].(map[string]any); return d }

	// Process A: submit, and watch the rounds until the payment is in flight.
	a := launch(t, "A", db, ledger, true)
	a.send(map[string]any{"id": 1, "kind": "subscribe", "from_seq": 0})
	a.send(map[string]any{"id": 2, "kind": "submit", "task_id": id, "input": prompt})
	payCall := ""
	deadline := time.Now().Add(5 * time.Minute)
	for len(ledgerEntries(t, ledger)) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no payment within 5 minutes; stderr:\n%s", a.stderr.String())
		}
		select {
		case v, ok := <-a.lines:
			if !ok {
				t.Fatalf("A: stdout ended; stderr:\n%s", a.stderr.String())
			}
			if v["type"] == "event" {
				t.Logf("[A] %s", show(v))
			} else if v["ok"] != true {
				t.Fatalf("A refused: %v", v)
			}
			if !isTask(v) {
				continue
			}
			switch v["kind"] {
			case "tool_call":
				if strings.HasSuffix(fmt.Sprint(data(v)["tool"]), "pay") {
					payCall = fmt.Sprint(data(v)["call_id"])
				}
			case "done", "failed", "cancelled":
				t.Fatalf("task ended before the payment was made: %s", show(v))
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	paid := ledgerEntries(t, ledger)
	if err := a.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	<-a.done
	t.Logf("=== SIGKILL sent to A (pid %d) while %s was executing; A exited: %v", a.cmd.Process.Pid, paid[0].Key, a.cmd.ProcessState)
	for range a.lines {
	}
	_, calls, lastA := snapshot(t, db, id, "after SIGKILL, before restart")
	if payCall == "" { // its event may not have reached us before the kill
		for _, c := range calls {
			if strings.HasSuffix(c.Tool, "pay") && c.Status == mem.CallDispatched {
				payCall = c.CallID
			}
		}
	}
	t.Logf("pay call in flight at the kill: %s", payCall)

	// Process B: restart on the same file. The payment must be held.
	b := launch(t, "B", db, ledger, false)
	b.send(map[string]any{"id": 1, "kind": "subscribe", "from_seq": 0})
	held := b.until(2*time.Minute, func(v map[string]any) bool { return isTask(v) && v["kind"] == "reconcile_needed" })
	hc, hk := fmt.Sprint(data(held)["call_id"]), fmt.Sprint(data(held)["key"])
	b.close()
	tcb, calls, lastB := snapshot(t, db, id, "after restart, before resolve")
	if tcb.State != task.Waiting || tcb.Wait != task.WaitReconcile {
		t.Fatalf("after restart the task is %s/%s, want waiting/reconcile", tcb.State, tcb.Wait)
	}
	var paidKeys []string
	for _, e := range ledgerEntries(t, ledger) {
		paidKeys = append(paidKeys, e.Key)
	}
	if !strings.Contains(strings.Join(paidKeys, " "), hk) {
		t.Fatalf("held call %s with key %s, but the ledger has %v", hc, hk, paidKeys)
	}

	// Process C: the operator finds the key in the ledger and resolves the
	// call with what really happened; the task must then complete.
	c := launch(t, "C", db, ledger, false)
	c.send(map[string]any{"id": 1, "kind": "subscribe", "from_seq": lastB})
	c.send(map[string]any{"id": 2, "kind": "resolve", "task_id": id, "call_id": hc,
		"outcome": map[string]any{"result": fmt.Sprintf("Paid %s EUR to %s, transaction id %s.", amount, payee, tx(hk))}})
	c.until(time.Minute, func(v map[string]any) bool { return v["type"] == "reply" && v["kind"] == "resolve" })
	end := c.until(5*time.Minute, func(v map[string]any) bool {
		return isTask(v) && (v["kind"] == "done" || v["kind"] == "failed" || v["kind"] == "cancelled")
	})
	c.close()
	tcb, calls, _ = snapshot(t, db, id, "after resolve")

	// The whole event log, seq-ordered, across the three processes.
	s, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	evs, err := s.Events(context.Background(), 0, 1<<20)
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("--- event log of %s (A: seq <= %d, B: <= %d, C: after)", id, lastA, lastB)
	// The round a call was made in is the number of model steps before its
	// tool_call event; the store has every event, even those committed just
	// before the kill that never reached the subscriber.
	answer, steps, payRound := "", 0, 0
	for _, ev := range evs {
		if ev.TaskID != id {
			continue
		}
		var d map[string]any
		json.Unmarshal(ev.Data, &d)
		switch ev.Kind {
		case "answer":
			answer = fmt.Sprint(d["text"])
		case "step":
			steps++
		case "tool_call":
			if d["call_id"] == payCall {
				payRound = steps
			}
		}
		proc := "A"
		if ev.Seq > lastB {
			proc = "C"
		} else if ev.Seq > lastA {
			proc = "B"
		}
		t.Logf("  %s %s", proc, show(map[string]any{"seq": ev.Seq, "task_seq": ev.TaskSeq, "kind": ev.Kind, "data": d}))
	}
	t.Logf("--- pay call %s was made in model round %d of %d", payCall, payRound, steps)
	final := ledgerEntries(t, ledger)
	t.Logf("--- ledger (%d line(s)):", len(final))
	for _, e := range final {
		t.Logf("    pid=%d key=%s tx=%s args=%s", e.PID, e.Key, e.TX, e.Args)
	}
	t.Logf("--- final answer: %s", answer)

	// Verdict.
	var pays []mem.CallRecord
	for _, c := range calls {
		if strings.HasSuffix(c.Tool, "pay") {
			pays = append(pays, c)
		}
	}
	if payRound < 2 {
		t.Errorf("the payment was made in model round %d: the scenario wants it after a lookup round", payRound)
	}
	if end["kind"] != "done" || tcb.State != task.Done {
		t.Errorf("task ended %v (%s/%q), want done", end["kind"], tcb.State, tcb.StopReason)
	}
	if hc != payCall {
		t.Errorf("held call %s, but the payment in flight was %s", hc, payCall)
	}
	if len(final) != 1 {
		// Tell a machine bug from a model choice: the same call executed twice
		// is the machine re-dispatching it; distinct calls are the model paying again.
		seen := map[string]bool{}
		for _, e := range final {
			parts := strings.Split(e.Key, "/") // task/call/attempt
			if len(parts) >= 2 && seen[parts[1]] {
				t.Errorf("MACHINE BUG: call %s executed more than once: %v", parts[1], final)
			}
			if len(parts) >= 2 {
				seen[parts[1]] = true
			}
		}
		t.Errorf("the ledger has %d payments, want 1 (pay calls in the log: %d)", len(final), len(pays))
	}
	for _, p := range pays {
		if p.CallID == hc && (p.Status != mem.CallDone || p.Attempt != 1 || p.Result == nil) {
			t.Errorf("held call after resolve: status %s attempt %d", p.Status, p.Attempt)
		}
	}
	if !strings.Contains(answer, tx(hk)) {
		t.Logf("note: the final answer does not quote the transaction id %s", tx(hk))
	}
}
