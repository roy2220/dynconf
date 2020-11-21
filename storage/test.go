package storage

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Factory is the type of the function createing a storage.
type Factory func() (storage Storage)

// Test tests storages created by the given factory.
func Test(t *testing.T, f Factory) {
	t.Run("GetValue", func(t *testing.T) { TestGetValue(t, f) })
	t.Run("WaitForValue", func(t *testing.T) { TestWaitForValue(t, f) })
	t.Run("DeleteValue", func(t *testing.T) { TestDeleteValue(t, f) })
	t.Run("RaceCondition", func(t *testing.T) { TestRaceCondition(t, f) })
}

func TestGetValue(t *testing.T, f Factory) {
	const N = 3
	type Input struct {
		Ctx context.Context
		Key string
	}
	type Output struct {
		Val    string
		Ver    Version
		ErrStr string
	}
	type TestCase struct {
		Line   int
		Setup  func(*testing.T, *TestCase, Storage)
		Input  Input
		Output Output
		State  Details
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				_, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				ver, err := s.DeleteValue(context.Background(), "foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, ver) {
					t.FailNow()
				}
				tc.State.Version = ver
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "123", "456")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tmp := tc.State.Key2Value["123"]
				tmp.Version = ver
				tc.State.Key2Value["123"] = tmp
				tc.State.Version = ver
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"123": {
						V: "456",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "123", "456")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tmp := tc.State.Key2Value["123"]
				tmp.Version = ver
				tc.State.Key2Value["123"] = tmp
				tc.State.Version = ver
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"123": {
						V: "456",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "123", "456")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tmp := tc.State.Key2Value["123"]
				tmp.Version = ver
				tc.State.Key2Value["123"] = tmp

				ver, err = s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tc.Output.Ver = ver
				tmp = tc.State.Key2Value["foo"]
				tmp.Version = ver
				tc.State.Key2Value["foo"] = tmp
				tc.State.Version = ver
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Val: "bar",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"123": {
						V: "456",
					},
					"foo": {
						V: "bar",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				err := s.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				ErrStr: ErrClosed.Error(),
			},
			State: Details{
				IsClosed: true,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			s := f()
			defer s.Close()
			if setup := tc.Setup; setup != nil {
				setup(t, tc, s)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					var err error
					output.Val, output.Ver, err = s.GetValue(tc.Input.Ctx, tc.Input.Key)
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			state, err := s.Inspect(context.Background())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestWaitForValue(t *testing.T, f Factory) {
	const N = 3
	type Input struct {
		Ctx context.Context
		Key string
		Ver Version
	}
	type Output struct {
		Val    string
		Ver    Version
		ErrStr string
	}
	type TestCase struct {
		Line    int
		Setup   func(*testing.T, *TestCase, Storage)
		Input   Input
		Output  Output
		State   Details
		Barrier chan struct{}
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				tc.Input.Ctx = ctx
				_ = cancel
			},
			Input: Input{
				Key: "foo",
			},
			Output: Output{
				ErrStr: context.DeadlineExceeded.Error(),
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tc.Output.Ver = ver
				tmp := tc.State.Key2Value["foo"]
				tmp.Version = ver
				tc.State.Key2Value["foo"] = tmp
				tc.State.Version = ver
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Val: "bar",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"foo": {
						V: "bar",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				ver, err = s.DeleteValue(context.Background(), "foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, ver) {
					t.FailNow()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				tc.Input.Ctx = ctx
				_ = cancel
				tc.State.Version = ver
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				ErrStr: context.DeadlineExceeded.Error(),
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				tc.Input.Ctx = ctx
				_ = cancel
				tc.Barrier = make(chan struct{})
				time.AfterFunc(50*time.Millisecond, func() {
					defer close(tc.Barrier)
					ver, err := s.SetValue(context.Background(), "123", "456")
					if !assert.NoError(t, err) {
						return
					}
					tmp := tc.State.Key2Value["123"]
					tmp.Version = ver
					tc.State.Key2Value["123"] = tmp
					tc.State.Version = ver
				})
			},
			Input: Input{
				Key: "foo",
			},
			Output: Output{
				ErrStr: context.DeadlineExceeded.Error(),
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"123": {
						V: "456",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				tc.Input.Ctx = ctx
				_ = cancel
				tc.Barrier = make(chan struct{})
				time.AfterFunc(50*time.Millisecond, func() {
					defer close(tc.Barrier)
					ver, err := s.SetValue(context.Background(), "foo", "bar")
					if !assert.NoError(t, err) {
						return
					}
					tc.Output.Ver = ver
					tmp := tc.State.Key2Value["foo"]
					tmp.Version = ver
					tc.State.Key2Value["foo"] = tmp
					tc.State.Version = ver
				})
			},
			Input: Input{
				Key: "foo",
			},
			Output: Output{
				Val: "bar",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"foo": {
						V: "bar",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				tc.Input.Ctx = ctx
				_ = cancel
				tc.Input.Ver = ver
				tmp := tc.State.Key2Value["foo"]
				tmp.Version = ver
				tc.State.Key2Value["foo"] = tmp
				tc.State.Version = ver
			},
			Input: Input{
				Key: "foo",
			},
			Output: Output{
				ErrStr: context.DeadlineExceeded.Error(),
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"foo": {
						V: "bar",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				tc.Input.Ctx = ctx
				_ = cancel
				tc.Input.Ver = ver
				tc.Barrier = make(chan struct{})
				time.AfterFunc(50*time.Millisecond, func() {
					defer close(tc.Barrier)
					ver, err := s.SetValue(context.Background(), "foo", "bar2")
					if !assert.NoError(t, err) {
						return
					}
					tc.Output.Ver = ver
					tmp := tc.State.Key2Value["foo"]
					tmp.Version = ver
					tc.State.Key2Value["foo"] = tmp
					tc.State.Version = ver
				})
			},
			Input: Input{
				Key: "foo",
			},
			Output: Output{
				Val: "bar2",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"foo": {
						V: "bar2",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tc.Input.Ver = ver
				time.AfterFunc(50*time.Millisecond, func() {
					ver, err := s.DeleteValue(context.Background(), "foo")
					if !assert.NoError(t, err) {
						return
					}
					if !assert.NotNil(t, ver) {
						return
					}
				})
				tc.Barrier = make(chan struct{})
				time.AfterFunc(100*time.Millisecond, func() {
					defer close(tc.Barrier)
					ver, err := s.SetValue(context.Background(), "foo", "baz")
					if !assert.NoError(t, err) {
						return
					}
					tc.Output.Ver = ver
					tmp := tc.State.Key2Value["foo"]
					tmp.Version = ver
					tc.State.Key2Value["foo"] = tmp
					tc.State.Version = ver
				})
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Val: "baz",
			},
			State: Details{
				Key2Value: map[string]ValueDetails{
					"foo": {
						V: "baz",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				err := s.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				ErrStr: ErrClosed.Error(),
			},
			State: Details{
				IsClosed: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				time.AfterFunc(50*time.Millisecond, func() {
					err := s.Close()
					assert.NoError(t, err)
				})
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				ErrStr: ErrClosed.Error(),
			},
			State: Details{
				IsClosed: true,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			s := f()
			defer s.Close()
			if setup := tc.Setup; setup != nil {
				setup(t, tc, s)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					var err error
					output.Val, output.Ver, err = s.WaitForValue(tc.Input.Ctx, tc.Input.Key, tc.Input.Ver)
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			if tc.Barrier != nil {
				<-tc.Barrier
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			state, err := s.Inspect(context.Background())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestDeleteValue(t *testing.T, f Factory) {
	const N = 3
	type Input struct {
		Ctx context.Context
		Key string
	}
	type Output struct {
		VerIsNil bool
		ErrStr   string
	}
	type State struct {
		Key2Val  map[string]ValueDetails
		VerIsNil bool
	}
	type TestCase struct {
		Line    int
		Setup   func(*testing.T, *TestCase, Storage)
		Input   Input
		Outputs map[Output]int
		State   State
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Outputs: map[Output]int{
				{VerIsNil: true}: N,
			},
			State: State{
				VerIsNil: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				_, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				_, err = s.DeleteValue(context.Background(), "foo")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Outputs: map[Output]int{
				{VerIsNil: true}: N,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				_, err := s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Outputs: map[Output]int{
				{}:               1,
				{VerIsNil: true}: N - 1,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				ver, err := s.SetValue(context.Background(), "123", "456")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				tmp := tc.State.Key2Val["123"]
				tmp.Version = ver
				tc.State.Key2Val["123"] = tmp
				_, err = s.SetValue(context.Background(), "foo", "bar")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Outputs: map[Output]int{
				{}:               1,
				{VerIsNil: true}: N - 1,
			},
			State: State{
				Key2Val: map[string]ValueDetails{
					"123": {
						V: "456",
					},
				},
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, s Storage) {
				err := s.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Outputs: map[Output]int{
				{VerIsNil: true, ErrStr: ErrClosed.Error()}: N,
			},
			State: State{
				VerIsNil: true,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			s := f()
			defer s.Close()
			if setup := tc.Setup; setup != nil {
				setup(t, tc, s)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					var ver Version
					var err error
					ver, err = s.DeleteValue(tc.Input.Ctx, tc.Input.Key)
					output.VerIsNil = ver == nil
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			outputs := make(map[Output]int, N)
			for i := 0; i < N; i++ {
				outputs[<-outputCh]++
			}
			assert.Equal(t, tc.Outputs, outputs)

			state, err := s.Inspect(context.Background())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.Equal(t, tc.State, State{state.Key2Value, state.Version == nil})
		})
	}
}

func TestRaceCondition(t *testing.T, f Factory) {
	const N = 33
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var kvs [3][2]string
	for i := 0; i < len(kvs); i++ {
		buf := [16]byte{}
		r.Read(buf[:])
		k := fmt.Sprintf("%X", buf)
		r.Read(buf[:])
		v := fmt.Sprintf("%X", buf)
		kvs[i] = [2]string{k, v}
	}
	s := f()
	defer s.Close()
	var lv atomic.Value
	exits := make(chan struct{}, N)
	for i := 0; i < N*N; i++ {
		go func() {
			defer func() {
				exits <- struct{}{}
			}()
			for i := 0; i < N; i++ {
				kv := kvs[rand.Intn(len(kvs))]
				switch rand.Intn(4) {
				case 0:
					val, ver, err := s.GetValue(context.Background(), kv[0])
					if !assert.NoError(t, err) {
						return
					}
					if ver == nil {
						return
					}
					assert.Equal(t, kv[1], val)
				case 1:
					val, ver, err := s.WaitForValue(context.Background(), kv[0], lv.Load())
					if !assert.NoError(t, err) {
						return
					}
					assert.Equal(t, kv[1], val)
					lv.Store(ver)
				case 2:
					ver, err := s.SetValue(context.Background(), kv[0], kv[1])
					if !assert.NoError(t, err) {
						return
					}
					lv.Store(ver)
				case 3:
					ver, err := s.DeleteValue(context.Background(), kv[0])
					if !assert.NoError(t, err) {
						return
					}
					if ver == nil {
						return
					}
					lv.Store(ver)
				default:
					panic("unreachable code")
				}
			}
		}()
	}
	for i := 0; i < N*N; i++ {
	Loop:
		for {
			select {
			case <-exits:
				break Loop
			case <-time.After(time.Second / 2):
				for _, kv := range kvs {
					ver, err := s.SetValue(context.Background(), kv[0], kv[1])
					if !assert.NoError(t, err) {
						return
					}
					lv.Store(ver)
				}
			}
		}
	}
}
