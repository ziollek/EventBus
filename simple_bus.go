package EventBus

import (
	"sync"
)

type SubscriptionRef uint64

// SimpleBusSubscriber defines subscription-related bus behavior for event of specific type T
type SimpleBusSubscriber[T any] interface {
	Subscribe(topic string, fn func(T)) SubscriptionRef
	SubscribeAsync(topic string, fn func(T), transactional bool) SubscriptionRef
	SubscribeOnce(topic string, fn func(T)) SubscriptionRef
	SubscribeOnceAsync(topic string, fn func(T)) SubscriptionRef
	Unsubscribe(topic string, ref SubscriptionRef)
}

// SimpleBusPublisher defines publishing-related bus behavior for event of specific type T
type SimpleBusPublisher[T any] interface {
	Publish(topic string, arg T)
}

// SimpleBus includes global (subscribe, publish, control) bus behavior
type SimpleBus[T any] interface {
	BusController
	SimpleBusSubscriber[T]
	SimpleBusPublisher[T]
}

type simpleHandler[T any] struct {
	callBack      func(T)
	once          bool
	async         bool
	transactional bool
	reference     SubscriptionRef
	sync.Mutex    // lock for an event handler - useful for running async callbacks serially
}

func (h *simpleHandler[T]) Call(arg T, wg *sync.WaitGroup) {
	if h.async {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if h.transactional {
				h.Lock()
				defer h.Unlock()
			}
			h.callBack(arg)
		}()
		return
	} else {
		h.callBack(arg)
	}
}

type listeners[T any] map[SubscriptionRef]*simpleHandler[T]

func (l listeners[T]) add(handler *simpleHandler[T]) {
	l[handler.reference] = handler
}

func (l listeners[_]) delete(ref SubscriptionRef) {
	delete(l, ref)
}

func newSimpleHandler[T any](fn func(T), ref SubscriptionRef, async, transactional, once bool) *simpleHandler[T] {
	return &simpleHandler[T]{
		callBack:      fn,
		reference:     ref,
		async:         async,
		transactional: transactional,
		once:          once,
	}
}

type TypedBus[T any] struct {
	handlers map[string]*listeners[T]
	lastRef  SubscriptionRef
	sync.RWMutex
	wg sync.WaitGroup
}

func NewSimpleBus[T any]() SimpleBus[T] {
	return &TypedBus[T]{
		handlers: make(map[string]*listeners[T]),
		lastRef:  SubscriptionRef(0),
	}
}

func (bus *TypedBus[_]) HasCallback(topic string) bool {
	bus.RLock()
	defer bus.RUnlock()
	_, ok := bus.handlers[topic]
	return ok && len(*bus.handlers[topic]) > 0
}

func (bus *TypedBus[T]) Subscribe(topic string, fn func(T)) SubscriptionRef {
	return bus.subscribe(topic, fn, false, false, false)
}

func (bus *TypedBus[T]) SubscribeOnce(topic string, fn func(T)) SubscriptionRef {
	return bus.subscribe(topic, fn, false, false, true)
}

func (bus *TypedBus[T]) SubscribeOnceAsync(topic string, fn func(T)) SubscriptionRef {
	return bus.subscribe(topic, fn, true, false, true)
}

func (bus *TypedBus[T]) SubscribeAsync(topic string, fn func(T), transactional bool) SubscriptionRef {
	return bus.subscribe(topic, fn, true, transactional, false)
}

func (bus *TypedBus[T]) Unsubscribe(topic string, ref SubscriptionRef) {
	bus.Lock()
	defer bus.Unlock()
	if _, ok := bus.handlers[topic]; ok {
		bus.handlers[topic].delete(ref)
	}
}

func (bus *TypedBus[T]) Publish(topic string, arg T) {
	if subscribers, ok := bus.fetchSubscribers(topic); ok {
		fire := make(chan *simpleHandler[T], len(*subscribers))
		bus.Lock()
		for _, subscriber := range *subscribers {
			fire <- subscriber
			if subscriber.once {
				bus.handlers[topic].delete(subscriber.reference)
			}
		}
		bus.Unlock()
		close(fire)
		// calling the callbacks will not block the whole bus
		for subscriber := range fire {
			subscriber.Call(arg, &bus.wg)
		}
	}
}

func (bus *TypedBus[T]) WaitAsync() {
	bus.wg.Wait()
}

func (bus *TypedBus[T]) subscribe(topic string, fn func(T), async, transactional, once bool) SubscriptionRef {
	bus.Lock()
	defer bus.Unlock()
	bus.lastRef++
	if _, ok := bus.handlers[topic]; !ok {
		bus.handlers[topic] = &listeners[T]{}
	}
	bus.handlers[topic].add(newSimpleHandler(fn, bus.lastRef, async, transactional, once))
	return bus.lastRef
}

func (bus *TypedBus[T]) fetchSubscribers(topic string) (*listeners[T], bool) {
	bus.RLock()
	defer bus.RUnlock()
	if handlers, ok := bus.handlers[topic]; ok && len(*handlers) > 0 {
		return handlers, true
	}
	return nil, false
}
