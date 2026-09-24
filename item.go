package ccache

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Sized interface {
	Size() int64
}

type TrackedItem[T any] interface {
	Value() T
	Release()
	Expired() bool
	TTL() time.Duration
	Expires() time.Time
	Extend(duration time.Duration)
}

type Item[T any] struct {
	gen        int64
	termSent   int32
	termReason int32
	key        string
	group      string
	promotions int32
	refCount   int32
	expires    int64
	size       int64
	value      T
	next       *Item[T]
	prev       *Item[T]
	inList     bool
}

func newItem[T any](key string, value T, expires int64, track bool) *Item[T] {
	size := int64(1)

	// https://github.com/golang/go/issues/49206
	if sized, ok := (interface{})(value).(Sized); ok {
		size = sized.Size()
	}

	item := &Item[T]{
		key:        key,
		value:      value,
		promotions: 0,
		size:       size,
		expires:    expires,
	}
	if track {
		item.refCount = 1
	}
	return item
}

func (i *Item[T]) shouldPromote(getsPerPromote int32) bool {
	i.promotions += 1
	return i.promotions == getsPerPromote
}

// Generation returns the per-key generation assigned when the item entered the
// cache. It starts at 1 for a key and increases on every successful Set of that
// key, which lets an event sink correlate transitions to a specific value
// generation without ever observing the value itself.
func (i *Item[T]) Generation() int64 {
	return i.gen
}

// markTerminal records why an item is leaving the cache. Only the first
// transition wins: a race between (for example) Delete and LRU eviction
// produces exactly one terminal event. Returns false if another transition
// already claimed the item.
func (i *Item[T]) markTerminal(reason EventReason) bool {
	return atomic.CompareAndSwapInt32(&i.termReason, 0, int32(reason))
}

// claimTerminal both records the terminal reason and reserves the right to
// emit the terminal event. Used when a transition is observed without going
// through markTerminal (e.g. an item sitting in the deletables queue).
func (i *Item[T]) claimTerminal(reason EventReason) (EventReason, bool) {
	if atomic.CompareAndSwapInt32(&i.termReason, 0, int32(reason)) {
		return reason, true
	}
	return EventReason(atomic.LoadInt32(&i.termReason)), false
}

func (i *Item[T]) isTerminal() bool {
	return atomic.LoadInt32(&i.termReason) != 0
}

func (i *Item[T]) terminalReason() EventReason {
	return EventReason(atomic.LoadInt32(&i.termReason))
}

func (i *Item[T]) Key() string {
	return i.key
}

func (i *Item[T]) Value() T {
	return i.value
}

func (i *Item[T]) track() {
	atomic.AddInt32(&i.refCount, 1)
}

func (i *Item[T]) Release() {
	atomic.AddInt32(&i.refCount, -1)
}

func (i *Item[T]) Expired() bool {
	expires := atomic.LoadInt64(&i.expires)
	return expires < time.Now().UnixNano()
}

func (i *Item[T]) TTL() time.Duration {
	expires := atomic.LoadInt64(&i.expires)
	return time.Nanosecond * time.Duration(expires-time.Now().UnixNano())
}

func (i *Item[T]) Expires() time.Time {
	expires := atomic.LoadInt64(&i.expires)
	return time.Unix(0, expires)
}

func (i *Item[T]) Extend(duration time.Duration) {
	atomic.StoreInt64(&i.expires, time.Now().Add(duration).UnixNano())
}

// String returns a string representation of the Item. This includes the default string
// representation of its Value(), as implemented by fmt.Sprintf with "%v", but the exact
// format of the string should not be relied on; it is provided only for debugging
// purposes, and because otherwise including an Item in a call to fmt.Printf or
// fmt.Sprintf expression could cause fields of the Item to be read in a non-thread-safe
// way.
func (i *Item[T]) String() string {
	group := i.group
	if group == "" {
		return fmt.Sprintf("Item(%s:%v)", i.key, i.value)
	}
	return fmt.Sprintf("Item(%s:%s:%v)", group, i.key, i.value)
}
