package EventBus

import (
	"reflect"
	"sync"
)

// SimpleBusSubscriber defines subscription-related bus behavior for event of specific type T
type SimpleBusSubscriber[T any] interface {
	Subscribe(topic string, fn func(T))
	SubscribeAsync(topic string, fn func(T), transactional bool)
	SubscribeOnce(topic string, fn func(T))
	SubscribeOnceAsync(topic string, fn func(T))
	Unsubscribe(topic string, handler func(T))
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

type callBack[T any] struct {
	Call    func(T)
	pointer uintptr
}

type genericHandler[T any] struct {
	callBack      callBack[T]
	once          bool
	async         bool
	transactional bool
	sync.Mutex    // lock for an event handler - useful for running async callbacks serially
}

func (h *genericHandler[T]) Call(arg T, wg *sync.WaitGroup) {
	if h.async {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if h.transactional {
				h.Lock()
				defer h.Unlock()
			}
			h.callBack.Call(arg)
		}()
		return
	} else {
		h.callBack.Call(arg)
	}
}

type listeners[T any] []*genericHandler[T]

func (l *listeners[T]) add(handler *genericHandler[T]) {
	for _, h := range *l {
		if h.callBack.pointer == handler.callBack.pointer {
			return
		}
	}
	*l = append(*l, handler)
}

func (l *listeners[T]) delete(handler *genericHandler[T]) {
	for i, h := range *l {
		if h.callBack.pointer == handler.callBack.pointer {
			*l = append((*l)[:i], (*l)[i+1:]...)
			return
		}
	}
}

func newGenericHandler[T any](fn func(T), async, transactional, once bool) *genericHandler[T] {
	return &genericHandler[T]{
		callBack:      callBack[T]{Call: fn, pointer: reflect.ValueOf(fn).Pointer()},
		async:         async,
		transactional: transactional,
		once:          once,
	}
}

type GenericBus[T any] struct {
	handlers map[string]*listeners[T]
	sync.RWMutex
	wg sync.WaitGroup
}

func NewSimpleBus[T any]() SimpleBus[T] {
	return &GenericBus[T]{
		handlers: make(map[string]*listeners[T]),
	}
}

func (bus *GenericBus[_]) HasCallback(topic string) bool {
	bus.RLock()
	defer bus.RUnlock()
	_, ok := bus.handlers[topic]
	return ok && len(*bus.handlers[topic]) > 0
}

func (bus *GenericBus[T]) Subscribe(topic string, fn func(T)) {
	bus.subscribe(topic, fn, false, false, false)
}

func (bus *GenericBus[T]) SubscribeOnce(topic string, fn func(T)) {
	bus.subscribe(topic, fn, false, false, true)
}

func (bus *GenericBus[T]) SubscribeOnceAsync(topic string, fn func(T)) {
	bus.subscribe(topic, fn, true, false, true)
}

func (bus *GenericBus[T]) SubscribeAsync(topic string, fn func(T), transactional bool) {
	bus.subscribe(topic, fn, true, transactional, false)
}

func (bus *GenericBus[T]) Unsubscribe(topic string, fn func(T)) {
	bus.Lock()
	defer bus.Unlock()
	if _, ok := bus.handlers[topic]; ok {
		bus.handlers[topic].delete(newGenericHandler(fn, false, false, false))
	}
}

func (bus *GenericBus[T]) Publish(topic string, arg T) {
	if subscribers, ok := bus.fetchSubscribers(topic); ok {
		fire := make(chan *genericHandler[T], len(*subscribers))
		bus.Lock()
		for _, subscriber := range *subscribers {
			fire <- subscriber
			if subscriber.once {
				bus.handlers[topic].delete(subscriber)
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

func (bus *GenericBus[T]) WaitAsync() {
	bus.wg.Wait()
}

func (bus *GenericBus[T]) subscribe(topic string, fn func(T), async, transactional, once bool) {
	bus.Lock()
	defer bus.Unlock()
	if _, ok := bus.handlers[topic]; !ok {
		bus.handlers[topic] = &listeners[T]{}
	}
	bus.handlers[topic].add(newGenericHandler(fn, async, transactional, once))
}

func (bus *GenericBus[T]) fetchSubscribers(topic string) (*listeners[T], bool) {
	bus.RLock()
	defer bus.RUnlock()
	if handlers, ok := bus.handlers[topic]; ok && len(*handlers) > 0 {
		return handlers, true
	}
	return nil, false
}
