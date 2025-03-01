package EventBus

import (
	"fmt"
	"testing"
	"time"
)

func TestNewGeneric(t *testing.T) {
	bus := NewSimpleBus[any]()
	if bus == nil {
		t.Log("New TypedBus not created!")
		t.Fail()
	}
}

func TestSimpleHasCallback(t *testing.T) {
	bus := NewSimpleBus[any]()
	bus.Subscribe("topic", func(_ any) {})
	if bus.HasCallback("topic_topic") {
		t.Fail()
	}
	if !bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSimpleOnceAndManySubscribe(t *testing.T) {
	bus := NewSimpleBus[any]()
	topic := "topic"
	flag := 0

	fn := func(_ any) { flag += 1 }

	refFirst := bus.SubscribeOnce(topic, fn)
	refSecond := bus.Subscribe(topic, fn)
	refThird := bus.Subscribe(topic, fn)

	if refFirst == refSecond || refFirst == refThird || refSecond == refThird {
		t.Fail()
	}

	bus.Publish(topic, nil)

	if flag != 3 {
		t.Fail()
	}
}

func TestSimpleUnsubscribe(t *testing.T) {
	bus := NewSimpleBus[any]()
	fn := func(_ any) {}
	ref := bus.Subscribe("topic", fn)
	if !bus.HasCallback("topic") {
		t.Logf("Expected to have callback for topic but it is not present")
		t.Fail()
	}
	bus.Unsubscribe("topic", ref)
	if bus.HasCallback("topic") {
		t.Logf("Expected to have no callback for topic after unsubscribe but it is present")
		t.Fail()
	}
}

type accumulator struct {
	val int
}

func (a *accumulator) Handle(_ any) {
	a.val++
}

func TestSimpleUnsubscribeMethod(t *testing.T) {
	bus := NewSimpleBus[any]()
	h := &accumulator{val: 0}

	ref := bus.Subscribe("topic", h.Handle)
	bus.Publish("topic", nil)
	bus.Unsubscribe("topic", ref)
	bus.Publish("topic", nil)

	if h.val != 1 {
		t.Fail()
	}
}

func TestSimplePublish(t *testing.T) {
	bus := NewSimpleBus[int]()
	bus.Subscribe("topic", func(a int) {
		if a != 10 {
			t.Fail()
		}
	})
	bus.Publish("topic", 10)
}

func TestSimpleSubscribeOnceAsync(t *testing.T) {
	value := 0

	bus := NewSimpleBus[*int]()
	bus.SubscribeOnceAsync("topic", func(a *int) {
		*a++
	})

	bus.Publish("topic", &value)
	bus.Publish("topic", &value)
	bus.WaitAsync()

	if value != 1 {
		t.Fail()
	}

	if bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSimpleSubscribeAsyncTransactional(t *testing.T) {
	started := make(chan struct{})
	defer close(started)
	results := make([]int, 0)
	type input struct {
		a   int
		out *[]int
		dur string
	}

	bus := NewSimpleBus[input]()
	bus.SubscribeAsync("topic", func(i input) {
		started <- struct{}{}
		sleep, _ := time.ParseDuration(i.dur)
		time.Sleep(sleep)
		*i.out = append(*i.out, i.a)
	}, true)

	bus.Publish("topic", input{a: 1, out: &results, dur: "1s"})
	// started is used to enforce that the first message is started being processed before the second is sent
	<-started
	bus.Publish("topic", input{a: 2, out: &results, dur: "0s"})
	<-started

	bus.WaitAsync()

	if len(results) != 2 {
		t.Logf("Expected 2 results, got %d", len(results))
		t.Fail()
	}

	if results[0] != 1 || results[1] != 2 {
		t.Logf("Expected [1 2] results, got %+v", results)
		t.Fail()
	}
}

func TestSimpleSubscribeAsync(t *testing.T) {
	ready := make(chan struct{})
	results := make(chan int)

	type input struct {
		a   int
		out chan<- int
	}

	bus := NewSimpleBus[input]()
	bus.SubscribeAsync("topic", func(i input) {
		i.out <- i.a
	}, false)

	bus.Publish("topic", input{a: 1, out: results})
	bus.Publish("topic", input{a: 2, out: results})

	numResults := 0

	go func() {
		for _ = range results {
			numResults++
		}
		close(ready)
	}()

	bus.WaitAsync()
	close(results)
	<-ready

	if numResults != 2 {
		t.Logf("Expected 2 results, got %d", numResults)
		t.Fail()
	}
}

func BenchmarkSimpleSynchronousPublishing(b *testing.B) {
	type input struct {
		number int
		slice  []int
		name   string
	}
	list := []int{1, 2, 3, 4, 5}

	bus := NewSimpleBus[input]()
	bus.Subscribe("topic", func(i input) {})

	b.ResetTimer()
	for range b.N {
		bus.Publish("topic", input{1, list, "test"})
	}
}

func BenchmarkSimpleAsynchronousPublishing(b *testing.B) {
	type input struct {
		number int
		slice  []int
		name   string
	}
	list := []int{1, 2, 3, 4, 5}

	bus := NewSimpleBus[input]()
	bus.SubscribeAsync("topic", func(i input) {}, false)

	b.ResetTimer()
	for range b.N {
		bus.Publish("topic", input{1, list, "test"})
	}
	bus.WaitAsync()
}
