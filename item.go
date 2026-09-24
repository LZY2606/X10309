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
	key        string
	group      string
	promotions int32
	refCount   int32
	expires    int64
	size       int64
	generation int64
	value      T
	next       *Item[T]
	prev       *Item[T]
	inList     bool
}

func newItem[T any](key string, value T, expires int64, generation int64, track bool) *Item[T] {
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
		generation: generation,
	}
	if track {
		item.refCount = 1
	}
	return item
}

func (i *Item[T]) shouldPromote(getsPerPromote int32) bool {
	return atomic.AddInt32(&i.promotions, 1) == getsPerPromote
}

func (i *Item[T]) Key() string {
	return i.key
}

// Generation is the dense, per-cache sequence number assigned when the item
// was stored. It uniquely identifies a specific value of a key, which lets an
// event trace distinguish an overwrite from the original generation.
func (i *Item[T]) Generation() int64 {
	return atomic.LoadInt64(&i.generation)
}

// markTerminal transitions an item into its removed state exactly once. It
// returns true only for the caller that performed the transition, so the
// single terminal event of a generation can be emitted by exactly one path.
func (i *Item[T]) markTerminal() bool {
	for {
		promotions := atomic.LoadInt32(&i.promotions)
		if promotions == -2 {
			return false
		}
		if atomic.CompareAndSwapInt32(&i.promotions, promotions, -2) {
			return true
		}
	}
}

func (i *Item[T]) terminal() bool {
	return atomic.LoadInt32(&i.promotions) == -2
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
