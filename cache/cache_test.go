package cache_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liliang-cn/neumann/cache"
	"github.com/liliang-cn/neumann/task"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)} }

func res(s string) task.ToolResult { return task.ToolResult{Content: s} }

// fill stores r under (taskID, tool, args) through a miss, failing if it hit.
func fill(t *testing.T, c *cache.L2, taskID, tool, args string, r task.ToolResult, ttl time.Duration) bool {
	t.Helper()
	got, hit, m := c.Lookup(taskID, tool, json.RawMessage(args))
	if hit {
		t.Fatalf("fill %s %s %s: unexpected hit %+v", taskID, tool, args, got)
	}
	return m.Fill(r, ttl)
}

func get(c *cache.L2, taskID, tool, args string) (task.ToolResult, bool) {
	r, hit, m := c.Lookup(taskID, tool, json.RawMessage(args))
	m.Drop()
	return r, hit
}

func TestHitReturnsTheStoredResult(t *testing.T) {
	c := cache.New(cache.Config{})
	if !fill(t, c, "t1", "dev_read", `{"path":"a"}`, res("A"), 0) {
		t.Fatal("fill refused")
	}
	r, ok := get(c, "t1", "dev_read", `{"path":"a"}`)
	if !ok || r != res("A") {
		t.Fatalf("got %+v, %v", r, ok)
	}
	if _, ok := get(c, "t1", "dev_read", `{"path":"b"}`); ok {
		t.Fatal("different args hit")
	}
	if _, ok := get(c, "t1", "dev_other", `{"path":"a"}`); ok {
		t.Fatal("different tool hit")
	}
	s := c.Stats()
	if s.Hits != 1 || s.Misses != 3 || s.Fills != 1 || s.Entries != 1 || s.Tasks != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestCanonicalArgs(t *testing.T) {
	same := [][2]string{
		{`{"a":1,"b":2}`, `{"b":2, "a":1}`},
		{`{"a":{"y":[1,2,{"q":1,"p":2}],"x":null}}`, " {\n \"a\" : {\"x\":null, \"y\":[1, 2, {\"p\":2,\"q\":1}]} } "},
		{`{"s":"A"}`, `{"s":"A"}`},
		{`[true,false,"x"]`, `[ true , false , "x" ]`},
	}
	for _, p := range same {
		k1, ok1 := cache.Canonical(json.RawMessage(p[0]))
		k2, ok2 := cache.Canonical(json.RawMessage(p[1]))
		if !ok1 || !ok2 || k1 != k2 {
			t.Errorf("%s / %s: %q(%v) != %q(%v)", p[0], p[1], k1, ok1, k2, ok2)
		}
	}
	differ := [][2]string{
		{`{"a":1}`, `{"a":1.0}`}, // numbers are kept as written: never a wrong hit
		{`{"a":1}`, `{"a":"1"}`},
		{`{"a":12345678901234567890}`, `{"a":12345678901234567891}`},
		{`[1,2]`, `[2,1]`},
		{`{"a":null}`, `{}`},
	}
	for _, p := range differ {
		k1, _ := cache.Canonical(json.RawMessage(p[0]))
		k2, _ := cache.Canonical(json.RawMessage(p[1]))
		if k1 == k2 {
			t.Errorf("%s / %s: both %q", p[0], p[1], k1)
		}
	}
	// Not JSON (or JSON with a repeated key, which devices read differently):
	// keyed by the raw bytes.
	for _, raw := range []string{`not json`, `{"a":1`, `{"a":1} x`, `{"a":1,"a":2}`, ``} {
		k, ok := cache.Canonical(json.RawMessage(raw))
		if ok || k != raw {
			t.Errorf("%q: got %q, %v", raw, k, ok)
		}
	}
}

func TestCanonicalArgsShareAnEntry(t *testing.T) {
	c := cache.New(cache.Config{})
	fill(t, c, "t1", "dev_read", `{"a":1,"b":2}`, res("X"), 0)
	if r, ok := get(c, "t1", "dev_read", `{"b":2, "a":1}`); !ok || r != res("X") {
		t.Fatalf("got %+v, %v", r, ok)
	}
	fill(t, c, "t1", "dev_read", `not json`, res("raw"), 0)
	if r, ok := get(c, "t1", "dev_read", `not json`); !ok || r != res("raw") {
		t.Fatalf("raw: got %+v, %v", r, ok)
	}
	if _, ok := get(c, "t1", "dev_read", `not  json`); ok {
		t.Fatal("raw args are matched byte for byte")
	}
}

func TestTTL(t *testing.T) {
	clk := newClock()
	c := cache.New(cache.Config{Now: clk.Now})
	fill(t, c, "t1", "dev_read", `{}`, res("short"), time.Minute)
	fill(t, c, "t1", "dev_forever", `{}`, res("forever"), 0)
	clk.Add(59 * time.Second)
	if _, ok := get(c, "t1", "dev_read", `{}`); !ok {
		t.Fatal("expired early")
	}
	clk.Add(time.Second)
	if _, ok := get(c, "t1", "dev_read", `{}`); ok {
		t.Fatal("served at its TTL")
	}
	clk.Add(1000 * time.Hour)
	if _, ok := get(c, "t1", "dev_forever", `{}`); !ok {
		t.Fatal("TTL 0 expired")
	}
	if s := c.Stats(); s.Expirations != 1 || s.Entries != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestTasksAreIsolated(t *testing.T) {
	c := cache.New(cache.Config{})
	fill(t, c, "t1", "dev_read", `{}`, res("one"), 0)
	if _, ok := get(c, "t2", "dev_read", `{}`); ok {
		t.Fatal("t2 saw t1's entry")
	}
	fill(t, c, "t2", "dev_read", `{}`, res("two"), 0)
	c.Invalidate("t2")
	if r, ok := get(c, "t1", "dev_read", `{}`); !ok || r != res("one") {
		t.Fatalf("t1 after invalidating t2: %+v, %v", r, ok)
	}
	if _, ok := get(c, "t2", "dev_read", `{}`); ok {
		t.Fatal("t2 survived its invalidation")
	}
}

// A read that was in flight when a write in its task completed may have seen
// the state before the write: it is not stored.
func TestInvalidationDropsAFillInFlight(t *testing.T) {
	c := cache.New(cache.Config{})
	_, _, m := c.Lookup("t1", "dev_read", json.RawMessage(`{}`))
	_, _, other := c.Lookup("t2", "dev_read", json.RawMessage(`{}`))
	c.Invalidate("t1")
	if m.Fill(res("stale"), 0) {
		t.Fatal("a fill that straddled an invalidation was stored")
	}
	if _, ok := get(c, "t1", "dev_read", `{}`); ok {
		t.Fatal("stale entry served")
	}
	if !other.Fill(res("fine"), 0) {
		t.Fatal("another task's fill was refused")
	}
	// After the invalidation a new miss fills normally.
	if !fill(t, c, "t1", "dev_read", `{}`, res("fresh"), 0) {
		t.Fatal("fresh fill refused")
	}
}

func TestForget(t *testing.T) {
	c := cache.New(cache.Config{})
	fill(t, c, "t1", "dev_read", `{"a":1}`, res("A"), 0)
	fill(t, c, "t1", "dev_read", `{"a":2}`, res("B"), 0)
	fill(t, c, "t2", "dev_read", `{"a":1}`, res("C"), 0)
	_, _, m := c.Lookup("t1", "dev_read", json.RawMessage(`{"a":3}`))
	c.Forget("t1")
	if m.Fill(res("late"), 0) {
		t.Fatal("a fill after Forget resurrected the task")
	}
	s := c.Stats()
	if s.Entries != 1 || s.Tasks != 1 {
		t.Fatalf("stats after Forget %+v", s)
	}
	if _, ok := get(c, "t1", "dev_read", `{"a":1}`); ok {
		t.Fatal("forgotten entry served")
	}
	if _, ok := get(c, "t2", "dev_read", `{"a":1}`); !ok {
		t.Fatal("Forget dropped another task")
	}
	c.Forget("t2")
	c.Forget("nobody")
	if s := c.Stats(); s.Entries != 0 || s.Bytes != 0 || s.Tasks != 0 {
		t.Fatalf("stats after forgetting all %+v", s)
	}
}

// A miss that is dropped leaves nothing behind: no task record outlives its
// last entry and its last pending miss.
func TestDroppedMissLeavesNothing(t *testing.T) {
	c := cache.New(cache.Config{})
	for i := range 100 {
		_, _, m := c.Lookup(fmt.Sprintf("t%d", i), "dev_read", nil)
		m.Drop()
		m.Drop() // idempotent
		if m.Fill(res("x"), 0) {
			t.Fatal("Fill after Drop stored")
		}
	}
	if s := c.Stats(); s.Tasks != 0 || s.Entries != 0 {
		t.Fatalf("stats %+v", s)
	}
	var nilMiss *cache.Miss
	nilMiss.Drop() // a hit's miss is nil, and dropping it is safe
}

func TestPerTaskEntryCap(t *testing.T) {
	c := cache.New(cache.Config{MaxEntriesPerTask: 3})
	for i := range 3 {
		fill(t, c, "t1", "dev_read", fmt.Sprintf(`{"i":%d}`, i), res("x"), 0)
	}
	fill(t, c, "t2", "dev_read", `{"i":0}`, res("x"), 0)
	get(c, "t1", "dev_read", `{"i":0}`) // 0 is now the most recently used
	fill(t, c, "t1", "dev_read", `{"i":3}`, res("x"), 0)
	if _, ok := get(c, "t1", "dev_read", `{"i":1}`); ok {
		t.Fatal("the least recently used entry survived")
	}
	for _, i := range []int{0, 2, 3} {
		if _, ok := get(c, "t1", "dev_read", fmt.Sprintf(`{"i":%d}`, i)); !ok {
			t.Fatalf("entry %d evicted", i)
		}
	}
	if _, ok := get(c, "t2", "dev_read", `{"i":0}`); !ok {
		t.Fatal("another task's entry evicted by t1's cap")
	}
	if s := c.Stats(); s.Evictions != 1 || s.Entries != 4 {
		t.Fatalf("stats %+v", s)
	}
}

func TestGlobalByteCapEvictsLeastRecentlyUsed(t *testing.T) {
	big := strings.Repeat("x", 1000)
	one := int64(len(big)) + 2000 // generous bound on one entry's accounted size
	c := cache.New(cache.Config{MaxBytes: 3 * one})
	fill(t, c, "t1", "dev_read", `{"i":1}`, res(big), 0)
	fill(t, c, "t2", "dev_read", `{"i":2}`, res(big), 0)
	fill(t, c, "t3", "dev_read", `{"i":3}`, res(big), 0)
	get(c, "t1", "dev_read", `{"i":1}`)
	for i := 4; i < 20; i++ {
		fill(t, c, fmt.Sprintf("t%d", i), "dev_read", fmt.Sprintf(`{"i":%d}`, i), res(big), 0)
		get(c, "t1", "dev_read", `{"i":1}`) // keep t1's entry hot
		if s := c.Stats(); s.Bytes > 3*one {
			t.Fatalf("over the byte cap: %+v", s)
		}
	}
	if _, ok := get(c, "t1", "dev_read", `{"i":1}`); !ok {
		t.Fatal("the hot entry was evicted")
	}
	if _, ok := get(c, "t2", "dev_read", `{"i":2}`); ok {
		t.Fatal("a cold entry survived")
	}
	s := c.Stats()
	if s.Evictions == 0 || s.Tasks != s.Entries {
		t.Fatalf("stats %+v (an evicted task's record must go with its last entry)", s)
	}
	// An entry larger than the whole cache is not stored at all.
	c2 := cache.New(cache.Config{MaxBytes: 100})
	if fill(t, c2, "t1", "dev_read", `{}`, res(big), 0) {
		t.Fatal("an entry over the byte cap was stored")
	}
}

func TestIsErrorIsNotStored(t *testing.T) {
	c := cache.New(cache.Config{})
	if fill(t, c, "t1", "dev_read", `{}`, task.ToolResult{Content: "boom", IsError: true}, 0) {
		t.Fatal("an error result was stored")
	}
	if _, ok := get(c, "t1", "dev_read", `{}`); ok {
		t.Fatal("an error result was served")
	}
}

func TestDefaults(t *testing.T) {
	if cache.DefaultMaxEntriesPerTask <= 0 || cache.DefaultMaxBytes <= 0 {
		t.Fatal("defaults must be positive")
	}
	if s := cache.New(cache.Config{}).Stats(); s.MaxBytes != cache.DefaultMaxBytes || s.MaxEntriesPerTask != cache.DefaultMaxEntriesPerTask {
		t.Fatalf("stats %+v", s)
	}
}

func TestConcurrentUse(t *testing.T) {
	c := cache.New(cache.Config{MaxEntriesPerTask: 8, MaxBytes: 64 << 10})
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				tid := fmt.Sprintf("t%d", (g+i)%5)
				args := fmt.Sprintf(`{"k":%d}`, i%13)
				r, hit, m := c.Lookup(tid, "dev_read", json.RawMessage(args))
				if hit {
					if r.Content != tid+args {
						panic(fmt.Sprintf("cross-task or cross-key hit: %q for %s %s", r.Content, tid, args))
					}
					continue
				}
				switch i % 7 {
				case 0:
					c.Invalidate(tid)
					m.Drop()
				case 1:
					c.Forget(tid)
					m.Drop()
				default:
					m.Fill(res(tid+args), time.Duration(i%3)*time.Second)
				}
				_ = c.Stats()
			}
		}()
	}
	wg.Wait()
	s := c.Stats()
	if s.Bytes > 64<<10 {
		t.Fatalf("over the byte cap: %+v", s)
	}
	for i := range 5 {
		c.Forget(fmt.Sprintf("t%d", i))
	}
	if s := c.Stats(); s.Entries != 0 || s.Bytes != 0 || s.Tasks != 0 {
		t.Fatalf("not empty after forgetting every task: %+v", s)
	}
}
