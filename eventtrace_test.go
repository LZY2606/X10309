// Regression material for "ccache: optional eviction and promotion event
// trajectory". All scenarios are driven exclusively through the package's
// public API (New/Layered + Configure(...).Events(...) + Set/Get/Delete/GC/
// Stop ...). Internal fields are read only to assert invariants; no internal
// terminal object is ever synthesized.
//
// Causality is checked with barriers (SyncUpdates + eventHub.barrier), never
// by assuming a fixed goroutine schedule, and never via map iteration order.
package ccache

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// eventRecorder is an EventSink that keeps every delivered event. It is safe
// to call back into the cache from record.
type eventRecorder struct {
	mu     sync.Mutex
	events []Event
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{}
}

func (r *eventRecorder) OnEvent(event Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *eventRecorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

func (r *eventRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// barrier waits until the sink has drained every event enqueued before it.
func (r *eventRecorder) barrier(c *Cache[any]) {
	c.SyncUpdates()
	c.events.barrier()
}

func (r *eventRecorder) barrierLayered(c *LayeredCache[any]) {
	c.SyncUpdates()
	c.events.barrier()
}

type eventKey struct {
	Type       EventType
	Reason     EventReason
	Key        string
	Group      string
	Generation int64
}

func keyOf(event Event) eventKey {
	return eventKey{event.Type, event.Reason, event.Key, event.Group, event.Generation}
}

func eventTypeName(t EventType) string {
	switch t {
	case EventSet:
		return "set"
	case EventPromotion:
		return "promotion"
	case EventExpiration:
		return "expiration"
	case EventDelete:
		return "delete"
	case EventReplace:
		return "replace"
	case EventEviction:
		return "eviction"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

func reasonName(r EventReason) string {
	switch r {
	case ReasonCreated:
		return "created"
	case ReasonReplaced:
		return "replaced"
	case ReasonInserted:
		return "inserted"
	case ReasonAccessed:
		return "accessed"
	case ReasonDeleted:
		return "deleted"
	case ReasonOverwritten:
		return "overwritten"
	case ReasonExpired:
		return "expired"
	case ReasonCapacity:
		return "capacity"
	case ReasonPruned:
		return "pruned"
	default:
		return fmt.Sprintf("unknown(%d)", r)
	}
}

func (k eventKey) String() string {
	where := k.Key
	if k.Group != "" {
		where = k.Group + "/" + k.Key
	}
	return fmt.Sprintf("%s(%s) %s#%d", eventTypeName(k.Type), reasonName(k.Reason), where, k.Generation)
}

func describeTrace(events []Event) string {
	var b strings.Builder
	for i, event := range events {
		fmt.Fprintf(&b, "\n  [%d] %s", i, keyOf(event))
	}
	return b.String()
}

// traceViolation pinpoints the first index at which an expectation breaks,
// instead of reporting a pass/fail boolean over the whole trace.
type traceViolation struct {
	index   int
	message string
}

func (v traceViolation) detail(events []Event) string {
	ctx := "<none>"
	if v.index < len(events) {
		ctx = fmt.Sprintf("[%d] %s", v.index, keyOf(events[v.index]))
	}
	return fmt.Sprintf("first violation at event %s: %s; trace:%s", ctx, v.message, describeTrace(events))
}

// expectedEvent is one member of an ordered, scheduling-independent
// expectation. Counts assert a cardinality; before asserts a happens-before
// relation that must hold for the same (key, generation).
type expectedEvent struct {
	key    eventKey
	count  int
	before []eventKey
}

// checkTrace validates cardinality and per-generation causality. "before"
// pairs are checked by earliest index: events do not have a single global
// order across independent keys, so no cross-key ordering is imposed.
func checkTrace(events []Event, expected []expectedEvent) *traceViolation {
	counts := make(map[eventKey]int)
	first := make(map[eventKey]int)
	for i, event := range events {
		k := keyOf(event)
		counts[k]++
		if _, exists := first[k]; !exists {
			first[k] = i
		}
	}
	for _, want := range expected {
		if got := counts[want.key]; got != want.count {
			return &traceViolation{
				index:   first[want.key],
				message: fmt.Sprintf("expected %d x %s, observed %d", want.count, want.key, got),
			}
		}
		for _, later := range want.before {
			if later.Key != want.key.Key || later.Group != want.key.Group {
				return &traceViolation{index: 0, message: "malformed expectation: causality pair spans keys"}
			}
			bi, oi := first[want.key], first[later]
			if _, exists := first[later]; !exists {
				return &traceViolation{index: bi, message: fmt.Sprintf("%s requires later %s which is absent", want.key, later)}
			}
			if !(bi < oi) {
				return &traceViolation{index: oi, message: fmt.Sprintf("%s@%d must happen before %s@%d", want.key, bi, later, oi)}
			}
		}
	}
	return nil
}

// terminalEvents returns terminal events, grouped by a stable item identity.
func terminalGroups(events []Event) map[eventKey]int {
	out := make(map[eventKey]int)
	for _, event := range events {
		switch event.Type {
		case EventDelete, EventReplace, EventExpiration, EventEviction:
			out[keyOf(event)]++
		}
	}
	return out
}

// canonicalTrace strips scheduling-dependent ordering: sort by group, key,
// generation then event type. Two schedules over the same script must produce
// identical canonical traces.
func canonicalTrace(events []Event) []eventKey {
	keys := make([]eventKey, 0, len(events))
	for _, event := range events {
		keys = append(keys, keyOf(event))
	}
	sort.SliceStable(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		if a.Generation != b.Generation {
			return a.Generation < b.Generation
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		return a.Reason < b.Reason
	})
	return keys
}

func assertTrace(t *testing.T, events []Event, expected []expectedEvent) {
	t.Helper()
	if v := checkTrace(events, expected); v != nil {
		t.Fatalf("%s", v.detail(events))
	}
}

func assertCanonicalEqual(t *testing.T, a, b []eventKey) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("canonical traces differ in length %d vs %d:\na:%s\nb:%s", len(a), len(b), describeCanonical(a), describeCanonical(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("first canonical divergence at position %d: %s vs %s\na:%s\nb:%s", i, a[i], b[i], describeCanonical(a), describeCanonical(b))
		}
	}
}

func describeCanonical(keys []eventKey) string {
	var b strings.Builder
	for i, k := range keys {
		fmt.Fprintf(&b, "\n  [%d] %s", i, k)
	}
	return b.String()
}

func setEvent(key string, gen int64, reason EventReason) eventKey {
	return eventKey{Type: EventSet, Reason: reason, Key: key, Generation: gen}
}

func promotionEvent(key string, gen int64, reason EventReason) eventKey {
	return eventKey{Type: EventPromotion, Reason: reason, Key: key, Generation: gen}
}

func terminalEventKey(kind EventType, key string, gen int64, reason EventReason) eventKey {
	return eventKey{Type: kind, Reason: reason, Key: key, Generation: gen}
}

var _ = time.Now

func keyN(i int) string {
	return "k" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
