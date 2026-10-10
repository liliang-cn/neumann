package bus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/bus"
	"github.com/liliang-cn/neumann/bus/builtin"
	"github.com/liliang-cn/neumann/cache"
	"github.com/liliang-cn/neumann/task"
)

// fs is a device with a cacheable read whose answer is the current version of
// a file, an uncacheable read, a write and an idempotent put. Every call is
// counted per tool.
type fs struct {
	mu      sync.Mutex
	version map[string]int
	calls   map[string]*atomic.Int64
	dev     *builtin.Device
}

func newFS(name string, ttl time.Duration) *fs {
	f := &fs{version: map[string]int{}, calls: map[string]*atomic.Int64{}}
	for _, n := range []string{"read", "peek", "write", "put", "fail", "nojson"} {
		f.calls[n] = &atomic.Int64{}
	}
	path := func(c bus.DeviceCall) string {
		var a struct{ Path string }
		_ = json.Unmarshal(c.Args, &a)
		return a.Path
	}
	read := func(n string) builtin.Func {
		return func(_ context.Context, c bus.DeviceCall) (string, error) {
			f.calls[n].Add(1)
			f.mu.Lock()
			defer f.mu.Unlock()
			return fmt.Sprintf("%s@%d", path(c), f.version[path(c)]), nil
		}
	}
	bump := func(n string) builtin.Func {
		return func(_ context.Context, c bus.DeviceCall) (string, error) {
			f.calls[n].Add(1)
			f.mu.Lock()
			f.version[path(c)]++
			f.mu.Unlock()
			return "ok", nil
		}
	}
	f.dev = builtin.New(name).
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly, Cacheable: true, CacheTTL: ttl}, read("read")).
		Add(bus.ToolDef{Name: "peek", Effect: bus.ReadOnly}, read("peek")).
		Add(bus.ToolDef{Name: "write", Effect: bus.SideEffect, Cacheable: true}, bump("write")). // Cacheable is ignored off ReadOnly
		Add(bus.ToolDef{Name: "put", Effect: bus.Idempotent}, bump("put")).
		Add(bus.ToolDef{Name: "fail", Effect: bus.ReadOnly, Cacheable: true}, func(context.Context, bus.DeviceCall) (string, error) {
			f.calls["fail"].Add(1)
			return "", fmt.Errorf("transient")
		}).
		Add(bus.ToolDef{Name: "nojson", Effect: bus.ReadOnly, Cacheable: true}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			f.calls["nojson"].Add(1)
			return "raw:" + string(c.Args), nil
		})
	return f
}

func (f *fs) n(tool string) int64 { return f.calls[tool].Load() }

func key(taskID string, call int) string { return fmt.Sprintf("%s/c-%d/1", taskID, call) }

// run calls tool in taskID and returns the result and whether it was a hit.
func run(t *testing.T, b *bus.Bus, taskID, tool, args string) (task.ToolResult, bool) {
	t.Helper()
	res, how, cached := b.RunCached(ctx, tool, json.RawMessage(args), key(taskID, 1), nil, nil, nil)
	if how != bus.Returned {
		t.Fatalf("%s %s: outcome %v", tool, args, how)
	}
	return res, cached
}

func TestCacheHitReturnsTheSameResultAndTheDeviceRunsOnce(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	first, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`)
	if cached || first.Content != "a@0" {
		t.Fatalf("first: %+v cached=%v", first, cached)
	}
	second, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`)
	if !cached || second != first {
		t.Fatalf("second: %+v cached=%v", second, cached)
	}
	// Run itself reports a hit as an ordinary returned result.
	if res, how := b.Run(ctx, "fs_read", json.RawMessage(`{"path":"a"}`), key("t1", 3), nil, nil, nil); how != bus.Returned || res != first {
		t.Fatalf("Run: %+v %v", res, how)
	}
	if f.n("read") != 1 {
		t.Fatalf("device ran %d times", f.n("read"))
	}
	s := b.CacheStats()
	if s.Hits != 2 || s.Misses != 1 || s.Fills != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestCacheDifferentArgsMissAndCanonicalArgsHit(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	run(t, b, "t1", "fs_read", `{"path":"a","n":1}`)
	if _, cached := run(t, b, "t1", "fs_read", `{"path":"b","n":1}`); cached {
		t.Fatal("different args hit")
	}
	if _, cached := run(t, b, "t1", "fs_read", ` { "n" : 1 , "path" : "a" } `); !cached {
		t.Fatal("reordered keys missed")
	}
	if f.n("read") != 2 {
		t.Fatalf("device ran %d times", f.n("read"))
	}
	// Non-JSON args are keyed by their bytes.
	run(t, b, "t1", "fs_nojson", `plain words`)
	if _, cached := run(t, b, "t1", "fs_nojson", `plain words`); !cached {
		t.Fatal("identical raw args missed")
	}
	if _, cached := run(t, b, "t1", "fs_nojson", `plain  words`); cached {
		t.Fatal("different raw args hit")
	}
}

func TestCacheTTLWithInjectedClock(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC).UnixNano())
	f := newFS("fs", time.Minute)
	b := mustBus(t, 4, f.dev)
	b.SetCache(cache.New(cache.Config{Now: func() time.Time { return time.Unix(0, now.Load()) }}))
	run(t, b, "t1", "fs_read", `{"path":"a"}`)
	now.Add(int64(30 * time.Second))
	if _, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`); !cached {
		t.Fatal("expired before its TTL")
	}
	now.Add(int64(30 * time.Second))
	if _, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`); cached {
		t.Fatal("served at its TTL")
	}
	if f.n("read") != 2 {
		t.Fatalf("device ran %d times", f.n("read"))
	}
}

func TestCacheIsPerTask(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	run(t, b, "t1", "fs_read", `{"path":"a"}`)
	if _, cached := run(t, b, "t2", "fs_read", `{"path":"a"}`); cached {
		t.Fatal("t2 was served t1's result")
	}
	// A task id may itself contain '/': the scope is everything before
	// call/attempt.
	run(t, b, "org/t3", "fs_read", `{"path":"a"}`)
	if _, cached := run(t, b, "org/t3", "fs_read", `{"path":"a"}`); !cached {
		t.Fatal("a task id with a slash missed")
	}
	if _, cached := run(t, b, "t3", "fs_read", `{"path":"a"}`); cached {
		t.Fatal("t3 was served org/t3's result")
	}
}

func TestCacheIsClearedByACompletedWriteInTheSameTaskOnly(t *testing.T) {
	for _, writer := range []string{"fs_write", "fs_put"} {
		t.Run(writer, func(t *testing.T) {
			f := newFS("fs", 0)
			b := mustBus(t, 4, f.dev)
			run(t, b, "t1", "fs_read", `{"path":"a"}`)
			run(t, b, "t2", "fs_read", `{"path":"a"}`)

			run(t, b, "t2", writer, `{"path":"a"}`) // another task's write
			if _, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`); !cached {
				t.Fatal("a write in t2 cleared t1's cache")
			}
			run(t, b, "t1", writer, `{"path":"zzz"}`) // any write clears, whatever it touched
			res, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`)
			if cached {
				t.Fatal("served from cache after a write in the same task")
			}
			if res.Content != "a@1" {
				t.Fatalf("after the write: %+v", res)
			}
			if b.CacheStats().Invalidations == 0 {
				t.Fatalf("stats %+v", b.CacheStats())
			}
		})
	}
}

// Completion, not dispatch: a read that finished while a write was still
// running may be cached; the write clears it when it completes. A read still
// in flight when the write completes is not stored.
func TestCacheIsClearedWhenTheWriteCompletes(t *testing.T) {
	var version atomic.Int64
	writeIn, writeGo := make(chan struct{}), make(chan struct{})
	readIn, readGo := make(chan struct{}, 1), make(chan struct{}, 1)
	slowRead := atomic.Bool{}
	d := builtin.New("fs").
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly, Cacheable: true}, func(context.Context, bus.DeviceCall) (string, error) {
			v := version.Load()
			if slowRead.Load() {
				readIn <- struct{}{}
				<-readGo
			}
			return fmt.Sprint(v), nil
		}).
		Add(bus.ToolDef{Name: "write"}, func(context.Context, bus.DeviceCall) (string, error) {
			close(writeIn)
			<-writeGo
			version.Add(1)
			return "ok", nil
		})
	b := mustBus(t, 4, d)
	done := make(chan struct{})
	go func() { run(t, b, "t1", "fs_write", `{}`); close(done) }()
	<-writeIn
	run(t, b, "t1", "fs_read", `{}`) // completes while the write runs
	if _, cached := run(t, b, "t1", "fs_read", `{}`); !cached {
		t.Fatal("a dispatched but unfinished write cleared the cache")
	}
	slowRead.Store(true)
	readDone := make(chan task.ToolResult)
	go func() {
		res, _, _ := b.RunCached(ctx, "fs_read", json.RawMessage(`{"x":1}`), key("t1", 9), nil, nil, nil)
		readDone <- res
	}()
	<-readIn // that read has seen version 0
	close(writeGo)
	<-done
	readGo <- struct{}{}
	if res := <-readDone; res.Content != "0" {
		t.Fatalf("slow read: %+v", res)
	}
	slowRead.Store(false)
	if res, cached := run(t, b, "t1", "fs_read", `{}`); cached || res.Content != "1" {
		t.Fatalf("after the write completed: %+v cached=%v", res, cached)
	}
	if res, cached := run(t, b, "t1", "fs_read", `{"x":1}`); cached || res.Content != "1" {
		t.Fatalf("the read that straddled the write was stored: %+v cached=%v", res, cached)
	}
}

func TestCacheSkipsErrorsAndUncacheableReads(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	for range 2 {
		if res, cached := run(t, b, "t1", "fs_fail", `{}`); cached || !res.IsError {
			t.Fatalf("fail: %+v cached=%v", res, cached)
		}
		if _, cached := run(t, b, "t1", "fs_peek", `{"path":"a"}`); cached {
			t.Fatal("a non-Cacheable read was cached")
		}
	}
	if f.n("fail") != 2 || f.n("peek") != 2 {
		t.Fatalf("fail ran %d, peek ran %d", f.n("fail"), f.n("peek"))
	}
	// Neither a SideEffect tool marked Cacheable nor a write is ever cached.
	run(t, b, "t1", "fs_write", `{"path":"a"}`)
	if _, cached := run(t, b, "t1", "fs_write", `{"path":"a"}`); cached || f.n("write") != 2 {
		t.Fatalf("write cached=%v ran %d", cached, f.n("write"))
	}
	// A non-Cacheable read does not clear the cache either.
	run(t, b, "t1", "fs_read", `{"path":"a"}`)
	run(t, b, "t1", "fs_peek", `{"path":"a"}`)
	if _, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`); !cached {
		t.Fatal("a read cleared the cache")
	}
}

func TestCacheNeverServesADeniedCall(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	run(t, b, "t1", "fs_read", `{"path":"a"}`)
	for _, pt := range [][2][]string{{nil, {"fs_read"}}, {{"fs_peek"}, nil}} {
		res, how, cached := b.RunCached(ctx, "fs_read", json.RawMessage(`{"path":"a"}`), key("t1", 2), pt[0], pt[1], nil)
		if cached || !res.IsError || how != bus.Returned {
			t.Fatalf("allow=%v deny=%v: %+v %v cached=%v", pt[0], pt[1], res, how, cached)
		}
	}
	if s := b.CacheStats(); s.Hits != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestCacheWithoutATaskScopeIsBypassed(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	for _, k := range []string{"", "no-slashes", "/c-1/1", "t1/c-1"} {
		for range 2 {
			if _, _, cached := b.RunCached(ctx, "fs_read", json.RawMessage(`{"path":"a"}`), k, nil, nil, nil); cached {
				t.Fatalf("key %q was served from cache", k)
			}
		}
	}
	if f.n("read") != 8 {
		t.Fatalf("device ran %d times", f.n("read"))
	}
	if s := b.CacheStats(); s.Hits+s.Misses != 0 {
		t.Fatalf("stats %+v", s)
	}
	// Call (no late, no flag) uses the cache like Run.
	b.Call(ctx, "fs_read", json.RawMessage(`{}`), key("t1", 1), nil, nil)
	b.Call(ctx, "fs_read", json.RawMessage(`{}`), key("t1", 2), nil, nil)
	if f.n("read") != 9 {
		t.Fatalf("device ran %d times", f.n("read"))
	}
}

func TestCacheCanBeDisabledAndForgotten(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	run(t, b, "t1", "fs_read", `{"path":"a"}`)
	run(t, b, "t2", "fs_read", `{"path":"a"}`)
	b.ForgetTask("t1")
	if _, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`); cached {
		t.Fatal("served after ForgetTask")
	}
	if _, cached := run(t, b, "t2", "fs_read", `{"path":"a"}`); !cached {
		t.Fatal("ForgetTask dropped another task")
	}
	b.SetCache(nil)
	if _, cached := run(t, b, "t2", "fs_read", `{"path":"a"}`); cached {
		t.Fatal("served with the cache disabled")
	}
	b.ForgetTask("t2") // safe with no cache
	if s := b.CacheStats(); s != (cache.Stats{}) {
		t.Fatalf("stats with no cache %+v", s)
	}
}

func TestCacheEvictionCapsThroughTheBus(t *testing.T) {
	f := newFS("fs", 0)
	b := mustBus(t, 4, f.dev)
	b.SetCache(cache.New(cache.Config{MaxEntriesPerTask: 2}))
	for _, p := range []string{"a", "b", "c"} {
		run(t, b, "t1", "fs_read", fmt.Sprintf(`{"path":%q}`, p))
	}
	if _, cached := run(t, b, "t1", "fs_read", `{"path":"a"}`); cached {
		t.Fatal("the evicted entry was served")
	}
	if s := b.CacheStats(); s.Evictions == 0 || s.Entries > 2 {
		t.Fatalf("stats %+v", s)
	}
}

// A result that arrives after the caller gave up never fills the cache, and a
// timeout is not a result to keep.
func TestCacheIgnoresLateResultsAndTimeouts(t *testing.T) {
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	var n atomic.Int64
	d := builtin.New("fs").
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly, Cacheable: true}, func(context.Context, bus.DeviceCall) (string, error) {
			if n.Add(1) == 1 {
				entered <- struct{}{}
				<-gate
			}
			return "v", nil
		}).
		Add(bus.ToolDef{Name: "slow", Effect: bus.ReadOnly, Cacheable: true, Timeout: 20 * time.Millisecond}, func(context.Context, bus.DeviceCall) (string, error) {
			time.Sleep(60 * time.Millisecond)
			return "slow", nil
		})
	b := mustBus(t, 4, d)
	cctx, cancel := context.WithCancel(ctx)
	go func() { <-entered; cancel() }()
	late := make(chan task.ToolResult, 1)
	if _, how, _ := b.RunCached(cctx, "fs_read", nil, key("t1", 1), nil, nil, func(r task.ToolResult) { late <- r }); how != bus.Abandoned {
		t.Fatalf("got %v", how)
	}
	close(gate)
	<-late
	if _, cached := run(t, b, "t1", "fs_read", ``); cached {
		t.Fatal("a late result filled the cache")
	}
	for range 2 {
		if res, cached := run(t, b, "t1", "fs_slow", `{}`); cached || !res.IsError {
			t.Fatalf("timeout: %+v cached=%v", res, cached)
		}
	}
	time.Sleep(100 * time.Millisecond) // the timed-out calls return meanwhile
	if _, cached := run(t, b, "t1", "fs_slow", `{}`); cached {
		t.Fatal("a result returned after its timeout filled the cache")
	}
}

// A write the bus stopped waiting for may still take effect: it clears the
// cache when the bus gives up, and again if the device returns later.
func TestCacheIsClearedByAnAbandonedWrite(t *testing.T) {
	var version atomic.Int64
	gate, entered := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	d := builtin.New("fs").
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly, Cacheable: true}, func(context.Context, bus.DeviceCall) (string, error) {
			return fmt.Sprint(version.Load()), nil
		}).
		Add(bus.ToolDef{Name: "write"}, func(context.Context, bus.DeviceCall) (string, error) {
			close(entered)
			<-gate
			version.Add(1)
			return "ok", nil
		})
	b := mustBus(t, 4, d)
	run(t, b, "t1", "fs_read", `{}`)
	cctx, cancel := context.WithCancel(ctx)
	go func() { <-entered; cancel() }()
	if _, how, _ := b.RunCached(cctx, "fs_write", nil, key("t1", 2), nil, nil, func(task.ToolResult) { close(finished) }); how != bus.Abandoned {
		t.Fatalf("got %v", how)
	}
	if _, cached := run(t, b, "t1", "fs_read", `{}`); cached {
		t.Fatal("served after an abandoned write")
	}
	close(gate)
	<-finished
	if res, cached := run(t, b, "t1", "fs_read", `{}`); cached || res.Content != "1" {
		t.Fatalf("after the abandoned write finished: %+v cached=%v", res, cached)
	}
}

// Identical misses in flight together are not merged: each runs its device,
// and the next call hits.
func TestCacheConcurrentIdenticalMissesBothRun(t *testing.T) {
	var n atomic.Int64
	in := make(chan struct{}, 2)
	release := make(chan struct{})
	d := builtin.New("fs").Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly, Cacheable: true}, func(context.Context, bus.DeviceCall) (string, error) {
		n.Add(1)
		in <- struct{}{}
		<-release
		return "v", nil
	})
	b := mustBus(t, 4, d)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, cached := b.RunCached(ctx, "fs_read", json.RawMessage(`{}`), key("t1", i), nil, nil, nil); cached {
				t.Error("an in-flight miss was served from cache")
			}
		}()
	}
	<-in
	<-in
	close(release)
	wg.Wait()
	if n.Load() != 2 {
		t.Fatalf("device ran %d times", n.Load())
	}
	if _, cached := run(t, b, "t1", "fs_read", `{}`); !cached || n.Load() != 2 {
		t.Fatalf("no hit after both misses filled: cached=%v ran=%d", cached, n.Load())
	}
}

// Many tasks reading and writing at once. Every result a task sees must be
// one its own last completed write allows: a read in a task never returns a
// version older than the task's last completed write.
func TestCacheStress(t *testing.T) {
	type state struct {
		mu sync.Mutex
		v  map[string]int
	}
	st := &state{v: map[string]int{}}
	tid := func(c bus.DeviceCall) string { tk, _, _ := cutTask(c.Key); return tk }
	d := builtin.New("fs").
		Add(bus.ToolDef{Name: "read", Effect: bus.ReadOnly, Cacheable: true}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			return fmt.Sprintf("%s|%d", tid(c), st.v[tid(c)]), nil
		}).
		Add(bus.ToolDef{Name: "write", Effect: bus.Idempotent}, func(_ context.Context, c bus.DeviceCall) (string, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.v[tid(c)]++
			return fmt.Sprint(st.v[tid(c)]), nil
		})
	b := mustBus(t, 8, d)
	b.SetCache(cache.New(cache.Config{MaxEntriesPerTask: 4, MaxBytes: 8 << 10}))
	// completed[task] is the highest version a write in that task has returned.
	var completed [4]atomic.Int64
	var wg sync.WaitGroup
	for g := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			taskID := fmt.Sprintf("t%d", g%4)
			for i := range 300 {
				k := fmt.Sprintf("%s/c-%d-%d/1", taskID, g, i)
				switch i % 9 {
				case 0:
					res, _, _ := b.RunCached(ctx, "fs_write", nil, k, nil, nil, nil)
					v, _ := strconv.Atoi(res.Content)
					for c := &completed[g%4]; ; {
						old := c.Load()
						if int64(v) <= old || c.CompareAndSwap(old, int64(v)) {
							break
						}
					}
				case 1:
					if i%27 == 1 {
						b.ForgetTask(taskID)
					}
				default:
					floor := int(completed[g%4].Load())
					res, how, _ := b.RunCached(ctx, "fs_read", json.RawMessage(fmt.Sprintf(`{"k":%d}`, i%5)), k, nil, nil, nil)
					if how != bus.Returned || res.IsError {
						t.Errorf("read: %+v %v", res, how)
						return
					}
					got, vs, _ := strings.Cut(res.Content, "|")
					v, err := strconv.Atoi(vs)
					if err != nil || got != taskID {
						t.Errorf("task %s read %q", taskID, res.Content)
						return
					}
					// Writes in this task by other goroutines may complete while the
					// read runs, so the floor is what had completed before it began.
					if v < floor {
						t.Errorf("task %s read version %d after write %d completed", taskID, v, floor)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	s := b.CacheStats()
	if s.Hits == 0 || s.Misses == 0 || s.Invalidations == 0 {
		t.Fatalf("stress exercised nothing: %+v", s)
	}
	if s.Bytes > 8<<10 {
		t.Fatalf("over the byte cap: %+v", s)
	}
}

// cutTask splits task/call/attempt as the bus does.
func cutTask(k string) (taskID, call, attempt string) {
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '/' {
			attempt, k = k[i+1:], k[:i]
			break
		}
	}
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '/' {
			return k[:i], k[i+1:], attempt
		}
	}
	return "", k, attempt
}
