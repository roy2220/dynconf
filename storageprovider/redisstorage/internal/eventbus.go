package internal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

type EventBus struct {
	pubSub        *redis.PubSub
	options       EventBusOptions
	closure       chan struct{}
	watchedEvents map[string]*watchedEvent
	works         chan func()
	wg            sync.WaitGroup
	isClosed1     int32
	lock          sync.Mutex
}

type EventBusOptions struct {
	ChannelNamePrefix        string
	MaxWatchedEventIdleCount int
	MaxWatchedEventIdleTime  time.Duration
}

func (ebo *EventBusOptions) normalize() {
	if ebo.ChannelNamePrefix == "" {
		ebo.ChannelNamePrefix = "{dynconf}:"
	}
	if ebo.MaxWatchedEventIdleCount < 1 {
		ebo.MaxWatchedEventIdleCount = 3
	}
	if ebo.MaxWatchedEventIdleTime < 1 {
		ebo.MaxWatchedEventIdleTime = 3 * time.Minute
	}
}

func (eb *EventBus) Init(pubSub *redis.PubSub, options EventBusOptions) *EventBus {
	eb.pubSub = pubSub
	eb.options = options
	eb.options.normalize()
	eb.closure = make(chan struct{})
	eb.watchedEvents = make(map[string]*watchedEvent)
	eb.works = make(chan func(), 100)
	eb.wg.Add(1)
	go func() {
		eb.handleMessages()
		eb.wg.Done()
	}()
	eb.wg.Add(1)
	go func() {
		eb.doWorks()
		eb.wg.Done()
	}()
	return eb
}

func (eb *EventBus) handleMessages() {
	for {
		if eb.isClosed() {
			return
		}
		message, err := eb.pubSub.Receive(context.Background())
		if err != nil {
			continue
		}
		switch message := message.(type) {
		case *redis.Message:
			eventType := eb.channelNameToEventType(message.Channel)
			watchers, err := eb.removeWatchers(eventType)
			if err != nil {
				if err != ErrEventBusClosed {
					panic("unreachable code")
				}
				return
			}
			event := Event{
				IsValid: true,
				Data:    message.Payload,
			}
			for watcher := range watchers {
				watcher.fireEvent(event)
			}
		case *redis.Subscription:
			eventType := eb.channelNameToEventType(message.Channel)
			watchers, err := eb.removeWatchers(eventType)
			if err != nil {
				if err != ErrEventBusClosed {
					panic("unreachable code")
				}
				return
			}
			var event Event
			for watcher := range watchers {
				watcher.fireEvent(event)
			}
		default:
		}
	}
}

func (eb *EventBus) removeWatchers(eventType string) (map[*Watcher]struct{}, error) {
	eb.lock.Lock()
	if eb.isClosed() {
		eb.lock.Unlock()
		return nil, ErrEventBusClosed
	}
	watchedEvent, ok := eb.watchedEvents[eventType]
	if !ok {
		eb.lock.Unlock()
		return nil, nil
	}
	watchers := watchedEvent.Watchers
	if watchers == nil {
		watchedEvent.IdleCount++
		if !(watchedEvent.IdleCount <= eb.options.MaxWatchedEventIdleCount && watchedEvent.IdleDeadline.After(time.Now())) {
			eb.unwatchForEvent(watchedEvent, eventType)
		}
		eb.lock.Unlock()
		return nil, nil
	}
	eb.resetWatchedEvent(watchedEvent)
	eb.lock.Unlock()
	return watchers, nil
}

func (eb *EventBus) unwatchForEvent(watchedEvent *watchedEvent, eventType string) {
	delete(eb.watchedEvents, eventType)
	eb.works <- func() {
		channelName := eb.eventTypeToChannelName(eventType)
		// Based on the implementation of PubSub, it' safe to ignore the returned error here.
		_ = eb.pubSub.Unsubscribe(context.Background(), channelName)
	}
}

func (eb *EventBus) doWorks() {
	for work := range eb.works {
		if eb.isClosed() {
			return
		}
		work()
	}
}

func (eb *EventBus) Close() error {
	eb.lock.Lock()
	if eb.isClosed() {
		eb.lock.Unlock()
		return ErrEventBusClosed
	}
	atomic.StoreInt32(&eb.isClosed1, 1)
	eb.lock.Unlock()
	eb.pubSub.Close()
	close(eb.closure)
	close(eb.works)
	eb.wg.Wait()
	return nil
}

func (eb *EventBus) AddWatcher(eventType string) (*Watcher, error) {
	eb.lock.Lock()
	if eb.isClosed() {
		eb.lock.Unlock()
		return nil, ErrEventBusClosed
	}
	watchedEvent, ok := eb.watchedEvents[eventType]
	if !ok {
		watchedEvent = eb.watchForEvent(eventType)
	}
	watcher := new(Watcher).init(eb.closure)
	if watchedEvent.Watchers == nil {
		watchedEvent.Watchers = make(map[*Watcher]struct{})
	}
	watchedEvent.Watchers[watcher] = struct{}{}
	eb.lock.Unlock()
	return watcher, nil
}

func (eb *EventBus) watchForEvent(eventType string) *watchedEvent {
	var watchedEvent watchedEvent
	eb.watchedEvents[eventType] = &watchedEvent
	eb.works <- func() {
		channelName := eb.eventTypeToChannelName(eventType)
		// Based on the implementation of PubSub, it' safe to ignore the returned error here.
		_ = eb.pubSub.Subscribe(context.Background(), channelName)
	}
	return &watchedEvent
}

func (eb *EventBus) RemoveWatcher(eventType string, watcher *Watcher) error {
	eb.lock.Lock()
	if eb.isClosed() {
		eb.lock.Unlock()
		return ErrEventBusClosed
	}
	watchedEvent, ok := eb.watchedEvents[eventType]
	if !ok {
		eb.lock.Unlock()
		return nil
	}
	if _, ok := watchedEvent.Watchers[watcher]; !ok {
		eb.lock.Unlock()
		return nil
	}
	delete(watchedEvent.Watchers, watcher)
	if len(watchedEvent.Watchers) == 0 {
		eb.resetWatchedEvent(watchedEvent)
	}
	eb.lock.Unlock()
	return nil
}

type EventBusDetails struct {
	IsClosed      bool
	WatchedEvents map[string]WatchedEventDetails
}

type WatchedEventDetails struct {
	NumberOfWatchers int
}

func (eb *EventBus) Inspect() EventBusDetails {
	eb.lock.Lock()
	if eb.isClosed() {
		eb.lock.Unlock()
		return EventBusDetails{IsClosed: true}
	}
	var watchedEventDetailsSet map[string]WatchedEventDetails
	for eventType, watchedEvent := range eb.watchedEvents {
		if watchedEventDetailsSet == nil {
			watchedEventDetailsSet = make(map[string]WatchedEventDetails, len(eb.watchedEvents))
		}
		WatchedEventDetails := WatchedEventDetails{
			NumberOfWatchers: len(watchedEvent.Watchers),
		}
		watchedEventDetailsSet[eventType] = WatchedEventDetails
	}
	eb.lock.Unlock()
	return EventBusDetails{
		WatchedEvents: watchedEventDetailsSet,
	}
}

func (eb *EventBus) resetWatchedEvent(watchedEvent *watchedEvent) {
	watchedEvent.Watchers = nil
	watchedEvent.IdleDeadline = time.Now().Add(eb.options.MaxWatchedEventIdleTime)
	watchedEvent.IdleCount = 0
}

func (eb *EventBus) isClosed() bool {
	return atomic.LoadInt32(&eb.isClosed1) != 0
}

func (eb *EventBus) channelNameToEventType(channelName string) string {
	return channelName[len(eb.options.ChannelNamePrefix):]
}

func (eb *EventBus) eventTypeToChannelName(eventType string) string {
	return eb.options.ChannelNamePrefix + eventType
}

type Watcher struct {
	eventBusClosure <-chan struct{}
	signal          chan struct{}
	event           Event
}

func (w *Watcher) WaitForEvent(ctx context.Context) (Event, error) {
	select {
	case <-w.eventBusClosure:
		return Event{}, ErrEventBusClosed
	case <-w.signal:
		return w.event, nil
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}

func (w *Watcher) init(eventBusClosure <-chan struct{}) *Watcher {
	w.eventBusClosure = eventBusClosure
	w.signal = make(chan struct{})
	return w
}

func (w *Watcher) fireEvent(event Event) {
	w.event = event
	close(w.signal)
}

type Event struct {
	IsValid bool
	Data    string
}

var ErrEventBusClosed = errors.New("internal: event bus closed")

type watchedEvent struct {
	Watchers     map[*Watcher]struct{}
	IdleDeadline time.Time
	IdleCount    int
}
