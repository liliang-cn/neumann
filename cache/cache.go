// Package cache is L2: the read-only tool result cache.
//
// Its scope is one task. An entry is keyed by the full tool name and the
// canonical form of the arguments, and holds the real result an earlier call
// in the same task returned. The bus decides what may be cached (ReadOnly &&
// Cacheable, successful results only) and when a task's cache is cleared (a
// non-ReadOnly call in that task completed); this package only stores.
//
// Memory is bounded twice: at most MaxEntriesPerTask entries per task (the
// task's least recently used entry goes first) and at most MaxBytes across all
// tasks (the least recently used entry of any task goes first). Expired
// entries are dropped when they are looked up, or evicted like any other.
//
// A task's entries live until its TTLs run out, the caps evict them or the
// task is forgotten. The machine calls Forget when a task ends.
package cache

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/task"
)

const (
	// DefaultMaxEntriesPerTask bounds one task's entries.
	DefaultMaxEntriesPerTask = 256
	// DefaultMaxBytes bounds all entries together, in accounted bytes.
	DefaultMaxBytes int64 = 64 << 20
)

// entryOverhead is what one entry costs beyond its key and content: list
// elements, map slot, the entry itself. An estimate, so the byte cap is one.
const entryOverhead = 160

type Config struct {
	MaxEntriesPerTask int              // 0 = DefaultMaxEntriesPerTask
	MaxBytes          int64            // 0 = DefaultMaxBytes
	Now               func() time.Time // nil = time.Now
}

// Stats is a snapshot of the cache's counters and size.
type Stats struct {
	Hits          uint64 // lookups served from the cache
	Misses        uint64 // lookups that were not
	Fills         uint64 // results stored
	Evictions     uint64 // entries dropped by a cap
	Expirations   uint64 // entries dropped at their TTL
	Invalidations uint64 // task caches cleared by a write

	Entries           int
	Bytes             int64
	Tasks             int
	MaxEntriesPerTask int
	MaxBytes          int64
}

type L2 struct {
	maxEntries int
	maxBytes   int64
	now        func() time.Time

	mu    sync.Mutex
	tasks map[string]*taskCache
	lru   list.List // of *entry, most recently used first, all tasks
	stats Stats
}

type taskCache struct {
	id      string
	entries map[string]*entry
	lru     list.List // of *entry, this task's, most recently used first
	gen     uint64    // bumped by Invalidate: a miss from before cannot fill
	pending int       // misses that may still fill
	gone    bool      // dropped from the map: a miss holding it cannot fill
}

type entry struct {
	t       *taskCache
	key     string
	res     task.ToolResult
	expires time.Time // zero: no TTL
	size    int64
	global  *list.Element
	local   *list.Element
}

func New(cfg Config) *L2 {
	c := &L2{maxEntries: cfg.MaxEntriesPerTask, maxBytes: cfg.MaxBytes, now: cfg.Now, tasks: map[string]*taskCache{}}
	if c.maxEntries <= 0 {
		c.maxEntries = DefaultMaxEntriesPerTask
	}
	if c.maxBytes <= 0 {
		c.maxBytes = DefaultMaxBytes
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c
}

// Miss is a lookup that found nothing. Fill stores the result the call then
// returned; Drop gives up. Exactly one of them should follow every miss (both
// are safe to repeat, and on a nil Miss).
type Miss struct {
	c    *L2
	t    *taskCache
	key  string
	gen  uint64
	done bool
}

// Lookup returns a stored result for tool called with args in taskID, or a
// Miss to fill.
func (c *L2) Lookup(taskID, tool string, args json.RawMessage) (task.ToolResult, bool, *Miss) {
	key := entryKey(tool, args)
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tasks[taskID]
	if t != nil {
		if e := t.entries[key]; e != nil {
			if e.expires.IsZero() || c.now().Before(e.expires) {
				c.stats.Hits++
				t.lru.MoveToFront(e.local)
				c.lru.MoveToFront(e.global)
				return e.res, true, nil
			}
			c.stats.Expirations++
			c.remove(e)
		}
	}
	c.stats.Misses++
	if t == nil || t.gone {
		t = &taskCache{id: taskID, entries: map[string]*entry{}}
		c.tasks[taskID] = t
	}
	t.pending++
	return task.ToolResult{}, false, &Miss{c: c, t: t, key: key, gen: t.gen}
}

// Fill stores res, which the call that missed returned, for ttl (0: until the
// task's cache is cleared). It reports whether res was stored: an error
// result is not, nor one whose task was invalidated or forgotten since the
// lookup (it may describe the state before a write), nor one larger than the
// whole cache.
func (m *Miss) Fill(res task.ToolResult, ttl time.Duration) bool {
	if m == nil || m.done {
		return false
	}
	c, t := m.c, m.t
	c.mu.Lock()
	defer c.mu.Unlock()
	m.release()
	if res.IsError || t.gone || t.gen != m.gen || ttl < 0 {
		c.tidy(t)
		return false
	}
	size := int64(len(m.key)+len(res.Content)) + entryOverhead
	if size > c.maxBytes {
		c.tidy(t)
		return false
	}
	if old := t.entries[m.key]; old != nil {
		c.remove(old)
	}
	e := &entry{t: t, key: m.key, res: res, size: size}
	if ttl > 0 {
		e.expires = c.now().Add(ttl)
	}
	e.global = c.lru.PushFront(e)
	e.local = t.lru.PushFront(e)
	t.entries[m.key] = e
	c.stats.Fills++
	c.stats.Entries++
	c.stats.Bytes += size
	for len(t.entries) > c.maxEntries {
		c.evict(t.lru.Back().Value.(*entry))
	}
	for c.stats.Bytes > c.maxBytes {
		c.evict(c.lru.Back().Value.(*entry))
	}
	return true
}

// Drop releases a miss that will not be filled.
func (m *Miss) Drop() {
	if m == nil || m.done {
		return
	}
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	m.release()
	m.c.tidy(m.t)
}

func (m *Miss) release() {
	m.done = true
	m.t.pending--
}

// Invalidate clears taskID's cache, and keeps every miss already looked up in
// it from filling.
func (c *L2) Invalidate(taskID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tasks[taskID]
	if t == nil {
		return
	}
	c.stats.Invalidations++
	t.gen++
	for _, e := range t.entries {
		c.remove(e)
	}
	c.tidy(t)
}

// Forget drops taskID's cache for good: call it when the task ends. A miss
// still in flight for it cannot fill.
func (c *L2) Forget(taskID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tasks[taskID]
	if t == nil {
		return
	}
	for _, e := range t.entries {
		c.remove(e)
	}
	t.gone = true
	delete(c.tasks, taskID)
}

func (c *L2) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Tasks = len(c.tasks)
	s.MaxEntriesPerTask = c.maxEntries
	s.MaxBytes = c.maxBytes
	return s
}

func (c *L2) evict(e *entry) {
	c.stats.Evictions++
	c.remove(e)
	c.tidy(e.t)
}

// remove unlinks e; the caller tidies its task when done.
func (c *L2) remove(e *entry) {
	c.lru.Remove(e.global)
	e.t.lru.Remove(e.local)
	delete(e.t.entries, e.key)
	c.stats.Entries--
	c.stats.Bytes -= e.size
}

// tidy drops a task record that holds nothing and that no miss may fill.
func (c *L2) tidy(t *taskCache) {
	if !t.gone && len(t.entries) == 0 && t.pending == 0 {
		t.gone = true
		delete(c.tasks, t.id)
	}
}

// entryKey is the full tool name and the canonical args. Tool names never
// contain NUL.
func entryKey(tool string, args json.RawMessage) string {
	a, _ := Canonical(args)
	return tool + "\x00" + a
}

// Canonical is args as one JSON value with object keys sorted, insignificant
// whitespace removed and strings re-escaped one way; numbers stay as written,
// so 1 and 1.0 are different arguments, never a wrong hit. ok is false when
// args is not exactly one JSON value or repeats a key in an object (devices
// disagree on which one wins), and the key is then args' raw bytes.
func Canonical(args json.RawMessage) (key string, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(args))
	dec.UseNumber()
	var out bytes.Buffer
	if err := canonValue(dec, &out); err != nil {
		return string(args), false
	}
	if _, err := dec.Token(); err != io.EOF {
		return string(args), false
	}
	return out.String(), true
}

var errDuplicateKey = errors.New("cache: repeated object key")

func canonValue(dec *json.Decoder, out *bytes.Buffer) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			type member struct{ k, v string }
			var ms []member
			seen := map[string]bool{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return err
				}
				k, ok := kt.(string)
				if !ok {
					return errors.New("cache: object key is not a string")
				}
				if seen[k] {
					return errDuplicateKey
				}
				seen[k] = true
				var vb bytes.Buffer
				if err := canonValue(dec, &vb); err != nil {
					return err
				}
				ms = append(ms, member{k, vb.String()})
			}
			if _, err := dec.Token(); err != nil { // '}'
				return err
			}
			slices.SortFunc(ms, func(a, b member) int {
				switch {
				case a.k < b.k:
					return -1
				case a.k > b.k:
					return 1
				}
				return 0
			})
			out.WriteByte('{')
			for i, m := range ms {
				if i > 0 {
					out.WriteByte(',')
				}
				writeString(out, m.k)
				out.WriteByte(':')
				out.WriteString(m.v)
			}
			out.WriteByte('}')
		case '[':
			out.WriteByte('[')
			for i := 0; dec.More(); i++ {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := canonValue(dec, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // ']'
				return err
			}
			out.WriteByte(']')
		default:
			return errors.New("cache: unexpected delimiter")
		}
	case string:
		writeString(out, v)
	case json.Number:
		out.WriteString(v.String())
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case nil:
		out.WriteString("null")
	default:
		return errors.New("cache: unexpected token")
	}
	return nil
}

func writeString(out *bytes.Buffer, s string) {
	b, _ := json.Marshal(s) // a string always marshals
	out.Write(b)
}
