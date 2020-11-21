package internal_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	. "github.com/roy2220/dynconf/storageprovider/memorystorage/internal"
	"github.com/stretchr/testify/assert"
)

func TestValue_Get(t *testing.T) {
	const N = 3
	type Output struct {
		Val string
		Ver Version
	}
	type TestCase struct {
		Line   int
		Setup  func(*testing.T, *TestCase, *Value)
		Output Output
		State  ValueDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("abc", 99)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Output: Output{
				Val: "abc",
				Ver: 99,
			},
			State: ValueDetails{
				V:        "abc",
				Version:  99,
				RefCount: 1,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			var v Value
			if setup := tc.Setup; setup != nil {
				setup(t, tc, &v)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					output.Val, output.Ver = v.Get()
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestValue_WaitFor(t *testing.T) {
	const N = 3
	type Input struct {
		Ctx context.Context
		Ver Version
		WO  WaitOptions
	}
	type Output struct {
		Val    string
		Ver    Version
		ErrStr string
	}
	type TestCase struct {
		Line   int
		Setup  func(*testing.T, *TestCase, *Value)
		Input  Input
		Output Output
		State  ValueDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("abc", 999)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				ok := v.Clear(func() {})
				if !assert.True(t, ok) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
			},
			Output: Output{
				ErrStr: ErrValueIsDeleted.Error(),
			},
			State: ValueDetails{
				IsDeleted: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("abc", 999)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Ver: 998,
			},
			Output: Output{
				Val: "abc",
				Ver: 999,
			},
			State: ValueDetails{
				V:        "abc",
				Version:  999,
				RefCount: 1,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				tc.Input.Ctx = ctx
				_ = cancel
			},
			Output: Output{
				ErrStr: context.DeadlineExceeded.Error(),
			},
			State: ValueDetails{
				IsDeleted: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				c := make(chan struct{})
				tc.Input.WO.Interruption = c
				time.AfterFunc(10*time.Millisecond, func() {
					close(c)
				})
			},
			Input: Input{
				Ctx: context.Background(),
				WO: WaitOptions{
					InterruptedError: errors.New("interrupted"),
				},
			},
			Output: Output{
				ErrStr: "interrupted",
			},
			State: ValueDetails{
				IsDeleted: true,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				time.AfterFunc(10*time.Millisecond, func() {
					err := v.Set("123", 123)
					assert.NoError(t, err)
				})
			},
			Input: Input{
				Ctx: context.Background(),
			},
			Output: Output{
				Val: "123",
				Ver: 123,
			},
			State: ValueDetails{
				V:        "123",
				Version:  123,
				RefCount: 1,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("abc", 999)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				time.AfterFunc(10*time.Millisecond, func() {
					vd := v.Inspect()
					assert.Equal(t, N, vd.NumberOfWaiters)
					assert.Equal(t, N+1, vd.RefCount)
					ok := v.Clear(func() {})
					assert.True(t, ok)
				})
				time.AfterFunc(20*time.Millisecond, func() {
					err := v.Set("efg", 1000)
					assert.NoError(t, err)
				})
			},
			Input: Input{
				Ctx: context.Background(),
				Ver: 999,
			},
			Output: Output{
				Val: "efg",
				Ver: 1000,
			},
			State: ValueDetails{
				V:        "efg",
				Version:  1000,
				RefCount: 1,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			var v Value
			if setup := tc.Setup; setup != nil {
				setup(t, tc, &v)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					var err error
					output.Val, output.Ver, err = v.WaitFor(
						tc.Input.Ctx, tc.Input.Ver, tc.Input.WO, func() {})
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestValue_Set(t *testing.T) {
	const N = 3
	type Input struct {
		Val string
		Ver Version
	}
	type Output struct {
		ErrStr string
	}
	type TestCase struct {
		Line   int
		Setup  func(*testing.T, *TestCase, *Value)
		Input  Input
		Output Output
		State  ValueDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("abc", 999)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				ok := v.Clear(func() {})
				if !assert.True(t, ok) {
					t.FailNow()
				}
			},
			Input: Input{
				Val: "xyz",
				Ver: 789,
			},
			Output: Output{
				ErrStr: ErrValueIsDeleted.Error(),
			},
			State: ValueDetails{
				IsDeleted: true,
			},
		},
		{
			Line: line(),
			Input: Input{
				Val: "xyz",
				Ver: 789,
			},
			State: ValueDetails{
				V:        "xyz",
				Version:  789,
				RefCount: 1,
			},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("abc", 123)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Input: Input{
				Val: "xyz",
				Ver: 789,
			},
			State: ValueDetails{
				V:        "xyz",
				Version:  789,
				RefCount: 1,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			var v Value
			if setup := tc.Setup; setup != nil {
				setup(t, tc, &v)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					err := v.Set(tc.Input.Val, tc.Input.Ver)
					if err != nil {
						output.ErrStr = err.Error()
					}
					outputCh <- output
				}()
			}
			for i := 0; i < N; i++ {
				assert.Equal(t, tc.Output, <-outputCh)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestValue_Clear(t *testing.T) {
	const N = 3
	type Output struct {
		OK bool
	}
	type TestCase struct {
		Line    int
		Setup   func(*testing.T, *TestCase, *Value)
		Outputs map[Output]int
		State   ValueDetails
	}
	line := func() int { _, _, line, _ := runtime.Caller(1); return line }
	testCases := []TestCase{
		{
			Line: line(),
			Outputs: map[Output]int{
				{OK: false}: N,
			},
			State: ValueDetails{},
		},
		{
			Line: line(),
			Setup: func(t *testing.T, tc *TestCase, v *Value) {
				err := v.Set("aaa", 777)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			},
			Outputs: map[Output]int{
				{OK: true}:  1,
				{OK: false}: N - 1,
			},
			State: ValueDetails{
				IsDeleted: true,
			},
		},
	}
	for i := range testCases {
		tc := &testCases[i]
		t.Run(fmt.Sprintf("#%d-L%d", i+1, tc.Line), func(t *testing.T) {
			t.Parallel()

			var v Value
			if setup := tc.Setup; setup != nil {
				setup(t, tc, &v)
			}

			outputCh := make(chan Output, N)
			for i := 0; i < N; i++ {
				go func() {
					var output Output
					output.OK = v.Clear(func() {})
					outputCh <- output
				}()
			}
			outputs := make(map[Output]int, N)
			for i := 0; i < N; i++ {
				outputs[<-outputCh]++
			}
			assert.Equal(t, tc.Outputs, outputs)

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}
