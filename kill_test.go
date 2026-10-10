package neumann_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/liliang-cn/neumann"
	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/mem/sqlite"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/sim"
	"github.com/liliang-cn/neumann/task"
)

// ledger is the outside world for the kill test: a payment appends a line
// to a file, which no process crash can take back.
func ledger(path string, hang bool) bus.Device {
	return builtin.New("dev").
		Add(bus.ToolDef{Name: "pay"}, func(ctx context.Context, c bus.DeviceCall) (string, error) {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return "", err
			}
			f.WriteString(c.Key + "\n")
			f.Sync()
			f.Close()
			if hang {
				<-ctx.Done() // the process is killed while the call is in flight
			}
			return "paid", nil
		}).
		Add(bus.ToolDef{Name: "look", Effect: bus.ReadOnly}, func(context.Context, bus.DeviceCall) (string, error) {
			return "balance 10", nil
		})
}

// TestKillNineHelper is the child process of TestKillNine.
func TestKillNineHelper(t *testing.T) {
	db, book := os.Getenv("NEUMANN_KILL_DB"), os.Getenv("NEUMANN_KILL_LEDGER")
	if db == "" {
		t.Skip("child process of TestKillNine")
	}
	s, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: payModel}, Store: s, Devices: []bus.Device{ledger(book, true)}})
	if err != nil {
		t.Fatal(err)
	}
	m.Start(context.Background())
	if _, err := m.Submit(context.Background(), proto.Instruction{Kind: proto.KindSubmit, TaskID: "pay-1", Input: "pay the bill"}); err != nil {
		t.Fatal(err)
	}
	select {} // until killed
}

// TestKillNine is design §19 acceptance 2: a real process is killed with
// SIGKILL while a tool with side effects is running. The restarted machine
// does not run it again; it holds it for an operator, and once resolved the
// task finishes. The payment happened exactly once.
func TestKillNine(t *testing.T) {
	dir := t.TempDir()
	db, book := filepath.Join(dir, "neumann.db"), filepath.Join(dir, "ledger")
	child := exec.Command(os.Args[0], "-test.run=^TestKillNineHelper$", "-test.count=1")
	child.Env = append(os.Environ(), "NEUMANN_KILL_DB="+db, "NEUMANN_KILL_LEDGER="+book)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if b, _ := os.ReadFile(book); len(b) > 0 {
			break
		}
		if time.Now().After(deadline) {
			child.Process.Kill()
			t.Fatal("the child never made the payment")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := child.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	child.Wait()

	s, err := sqlite.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: payModel}, Store: s, Devices: []bus.Device{ledger(book, false)}, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	events := m.Events(ctx, 0)
	if err := m.Start(bg); err != nil {
		t.Fatal(err)
	}
	defer stop(t, m)
	held := false
	for ev := range events {
		if ev.Kind == proto.EvReconcileNeeded {
			var d struct {
				CallID string `json:"call_id"`
				Key    string `json:"key"`
			}
			json.Unmarshal(ev.Data, &d)
			b, _ := os.ReadFile(book)
			if !strings.Contains(string(b), d.Key) {
				t.Fatalf("held call %s with key %s, but the ledger shows %q", d.CallID, d.Key, b)
			}
			held = true
			if err := m.Resolve(bg, "pay-1", d.CallID, neumann.Outcome{Result: "paid"}); err != nil {
				t.Fatal(err)
			}
		}
		if ev.TaskID == "pay-1" && ev.Terminal() {
			break
		}
	}
	got, err := s.Load(bg, "pay-1")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(book)
	if !held || got.State != task.Done || strings.Count(string(b), "\n") != 1 {
		t.Fatalf("held=%v state=%s ledger=%q", held, got.State, b)
	}
}
