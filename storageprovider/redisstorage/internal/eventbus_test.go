package internal_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	. "github.com/roy2220/dynconf/storageprovider/redisstorage/internal"
	"github.com/stretchr/testify/assert"
)

func TestEventBus_AddWatcher(t *testing.T) {
	const N = 3
	type Input struct {
		EBO EventBusOptions
		ET  string
	}
	type Output struct {
		ErrStr string
	}
	type TestCase struct {
		Line   int
		Setup  func(*testing.T, *TestCase, *EventBus)
		Input  Input
		Output Output
		State  EventBusDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus) {
				addWatcherSkipInvalidEvent(t, eb, "foo")
			},
			Input: Input{
				ET: "foo",
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {
						NumberOfWatchers: N + 1,
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus) {
				eb.Close()
			},
			Input: Input{
				ET: "foo",
			},
			Output: Output{
				ErrStr: ErrEventBusClosed.Error(),
			},
			State: EventBusDetails{
				IsClosed: true,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			rc := newRedisClient(t)
			defer rc.Close()
			ps := rc.Subscribe(context.Background())
			eb := new(EventBus).Init(ps, tc.Input.EBO)
			defer eb.Close()
			if setup := tc.Setup; setup != nil {
				setup(t, tc, eb)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					_, err := eb.AddWatcher(tc.Input.ET)
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			state := eb.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestWatcher_RemoveWaiter(t *testing.T) {
	const N = 3
	type Input struct {
		EBO EventBusOptions
		ET  string
	}
	type Output struct {
		ErrStr string
	}
	type TestCase struct {
		Line     int
		Setup    func(*testing.T, *TestCase, *EventBus, redis.UniversalClient) *Watcher
		Input    Input
		Output   Output
		Teardown func(*testing.T, *TestCase, *EventBus, redis.UniversalClient)
		State    EventBusDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				return w
			},
			Input: Input{
				ET: "foo",
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				e, err := w.WaitForEvent(context.Background())
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.Equal(t, Event{}, e) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				ET: "foo",
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				return w
			},
			Input: Input{
				ET: "foo1",
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {
						NumberOfWatchers: 1,
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				return nil
			},
			Input: Input{
				ET: "foo",
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				eb.RemoveWatcher("foo", w)
				return w
			},
			Input: Input{
				ET: "foo",
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				eb.Close()
				return w
			},
			Input: Input{
				ET: "foo",
			},
			Output: Output{
				ErrStr: ErrEventBusClosed.Error(),
			},
			State: EventBusDetails{
				IsClosed: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 1,
					MaxWatchedEventIdleTime:  time.Hour,
				},
				ET: "foo",
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				c = rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				time.Sleep(10 * time.Millisecond)
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 1,
					MaxWatchedEventIdleTime:  time.Hour,
				},
				ET: "foo",
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				time.Sleep(10 * time.Millisecond)
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 100,
					MaxWatchedEventIdleTime:  15 * time.Millisecond,
				},
				ET: "foo",
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				time.Sleep(20 * time.Millisecond)
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				time.Sleep(10 * time.Millisecond)
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 100,
					MaxWatchedEventIdleTime:  15 * time.Millisecond,
				},
				ET: "foo",
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				time.Sleep(5 * time.Millisecond)
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			rc := newRedisClient(t)
			defer rc.Close()
			ps := rc.Subscribe(context.Background())
			eb := new(EventBus).Init(ps, tc.Input.EBO)
			defer eb.Close()
			w := tc.Setup(t, tc, eb, rc)

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					err := eb.RemoveWatcher(tc.Input.ET, w)
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			if teardown := tc.Teardown; teardown != nil {
				teardown(t, tc, eb, rc)
			}

			state := eb.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}
func TestWatcher_WaitForEvent(t *testing.T) {
	const N = 3
	type Input struct {
		EBO EventBusOptions
		Ctx context.Context
	}
	type Output struct {
		E      Event
		ErrStr string
	}
	type TestCase struct {
		Line     int
		Setup    func(*testing.T, *TestCase, *EventBus, redis.UniversalClient) *Watcher
		Input    Input
		Output   Output
		Teardown func(*testing.T, *TestCase, *EventBus, redis.UniversalClient)
		State    EventBusDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				Ctx: context.Background(),
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				tc.Input.Ctx = ctx
				_ = cancel
				return w
			},
			Output: Output{
				ErrStr: context.DeadlineExceeded.Error(),
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {
						NumberOfWatchers: 1,
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				time.AfterFunc(10*time.Millisecond, func() {
					eb.Close()
				})
				return w
			},
			Input: Input{
				Ctx: context.Background(),
			},
			Output: Output{
				ErrStr: ErrEventBusClosed.Error(),
			},
			State: EventBusDetails{
				IsClosed: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				time.AfterFunc(10*time.Millisecond, func() {
					c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
					assert.NoError(t, c.Err())
				})
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix: "haha",
				},
				Ctx: context.Background(),
			},
			Output: Output{
				E: Event{
					IsValid: true,
					Data:    "hello",
				},
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w := addWatcherSkipInvalidEvent(t, eb, "foo")
				time.AfterFunc(10*time.Millisecond, func() {
					eb.Close()
				})
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix: "haha",
				},
				Ctx: context.Background(),
			},
			Output: Output{
				ErrStr: ErrEventBusClosed.Error(),
			},
			State: EventBusDetails{
				IsClosed: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 1,
					MaxWatchedEventIdleTime:  time.Hour,
				},
				Ctx: context.Background(),
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				c = rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				time.Sleep(10 * time.Millisecond)
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 1,
					MaxWatchedEventIdleTime:  time.Hour,
				},
				Ctx: context.Background(),
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				time.Sleep(10 * time.Millisecond)
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 100,
					MaxWatchedEventIdleTime:  15 * time.Millisecond,
				},
				Ctx: context.Background(),
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				time.Sleep(20 * time.Millisecond)
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
				time.Sleep(10 * time.Millisecond)
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) *Watcher {
				w, err := eb.AddWatcher("foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				return w
			},
			Input: Input{
				EBO: EventBusOptions{
					ChannelNamePrefix:        "aaa",
					MaxWatchedEventIdleCount: 100,
					MaxWatchedEventIdleTime:  15 * time.Millisecond,
				},
				Ctx: context.Background(),
			},
			Teardown: func(t *testing.T, tc *TestCase, eb *EventBus, rc redis.UniversalClient) {
				time.Sleep(5 * time.Millisecond)
				c := rc.Publish(context.Background(), tc.Input.EBO.ChannelNamePrefix+"foo", "hello")
				if !assert.NoError(t, c.Err()) {
					t.FailNow()
				}
			},
			State: EventBusDetails{
				WatchedEvents: map[string]WatchedEventDetails{
					"foo": {},
				},
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			rc := newRedisClient(t)
			defer rc.Close()
			ps := rc.Subscribe(context.Background())
			eb := new(EventBus).Init(ps, tc.Input.EBO)
			defer eb.Close()
			w := tc.Setup(t, tc, eb, rc)

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					var err error
					output.E, err = w.WaitForEvent(tc.Input.Ctx)
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			if teardown := tc.Teardown; teardown != nil {
				teardown(t, tc, eb, rc)
			}

			state := eb.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

type redisClient struct {
	*redis.Client

	m *miniredis.Miniredis
}

func newRedisClient(t *testing.T) redisClient {
	m, err := miniredis.Run()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	client := redis.NewClient(&redis.Options{Addr: m.Addr()})
	return redisClient{
		Client: client,
		m:      m,
	}
}

func (rc redisClient) Close() error {
	err := rc.Client.Close()
	rc.m.Close()
	return err
}

func addWatcherSkipInvalidEvent(t *testing.T, eb *EventBus, et string) *Watcher {
	w, err := eb.AddWatcher(et)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	e, err := w.WaitForEvent(context.Background())
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.Equal(t, Event{}, e) {
		t.FailNow()
	}
	w, err = eb.AddWatcher(et)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return w
}
