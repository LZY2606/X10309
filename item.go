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

// itemGenerationCounter hands out unique, monotonically increasing item
// generations. A global counter is used so that items never need a back
// reference to their cache; for any given key the generations still arrive in
// strictly increasing order, which is all an EventSink relies on.
var itemGenerationCounter uint64

type Item[T any] struct {
	key        string
	group      string
	promotions int32
	refCount   int32
	expires    int64
	size       int64
	value      T
	generation uint64
	// terminal holds the EventKind that terminated the item generation, or 0
	// while the item is live. It is claimed with compare-and-swap so that an
	// item generation produces at most one terminal event.
	terminal int32
	next     *Item[T]
	prev     *Item[T]
	inList   bool
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
		generation: atomic.AddUint64(&itemGenerationCounter, 1),
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

func (i *Item[T]) Key() string {
	return i.key
}

func (i *Item[T]) Value() T {
	return i.value
}

// Generation returns the unique, monotonically increasing number assigned to
// this particular incarnation of the key. Each overwrite starts a new
// generation; it matches the Generation field of the item's events.
func (i *Item[T]) Generation() uint64 {
	return i.generation
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

// claimTerminal reports whether the caller is the first to terminate the item
// generation with the given event kind. Only a winning claim produces a
// terminal event.
func (i *Item[T]) claimTerminal(kind EventKind) bool {
	return atomic.CompareAndSwapInt32(&i.terminal, 0, int32(kind))
}

func (i *Item[T]) isTerminal() bool {
	return atomic.LoadInt32(&i.terminal) != 0
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
