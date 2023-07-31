package retry

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestVerifySettings(t *testing.T) {
	for _, tc := range []struct {
		desc          string
		settings      Settings
		expectedError string
	}{
		{
			desc:     "default settings",
			settings: DefaultSettings(),
		},
		{
			desc:          "initial backoff bad settings",
			settings:      Settings{},
			expectedError: "initial backoff must be set to >= 0, got 0s",
		},
		{
			desc:          "multiplier bad",
			settings:      Settings{InitialBackoff: time.Second},
			expectedError: "multiplier must be >= 1, got 0",
		},
		{
			desc:          "max backoff bad",
			settings:      Settings{InitialBackoff: time.Second, Multiplier: 5, MaxBackoff: time.Millisecond},
			expectedError: "initial backoff (1s) must be less than max backoff (1ms)",
		},
		{
			desc:     "everything valid",
			settings: Settings{InitialBackoff: time.Second, Multiplier: 5, MaxBackoff: time.Hour},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.settings.Verify()
			if tc.expectedError != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRetry_Do(t *testing.T) {
	errors := []error{errors.New("1"), errors.New("2"), errors.Newf("3")}
	for _, tc := range []struct {
		desc          string
		settings      Settings
		do            func() func() error
		expectedSeen  []error
		expectedFinal error
	}{
		{
			desc: "success",
			settings: Settings{
				InitialBackoff: time.Microsecond,
				Multiplier:     2,
			},
			do: func() func() error {
				return func() error {
					return nil
				}
			},
		},
		{
			desc: "failed once",
			settings: Settings{
				InitialBackoff: time.Microsecond,
				Multiplier:     2,
			},
			do: func() func() error {
				it := 0
				return func() error {
					it++
					if it == 1 {
						return errors[0]
					}
					return nil
				}
			},
			expectedSeen: []error{errors[0]},
		},
		{
			desc: "failed all",
			settings: Settings{
				InitialBackoff: time.Microsecond,
				Multiplier:     2,
				MaxRetries:     3,
			},
			do: func() func() error {
				it := 0
				return func() error {
					it++
					return errors[it-1]
				}
			},
			expectedSeen:  []error{errors[0], errors[1]},
			expectedFinal: errors[2],
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			r, err := NewRetry(tc.settings)
			require.NoError(t, err)

			doFunc := tc.do()
			var seenErrors []error
			err = r.Do(doFunc, func(err error) {
				seenErrors = append(seenErrors, err)
			})
			require.Equal(t, tc.expectedSeen, seenErrors)
			require.Equal(t, tc.expectedFinal, err)
		})
	}
}

func TestRetry(t *testing.T) {
	startTime := time.Date(2020, 01, 01, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		desc             string
		settings         Settings
		expectedNext     []time.Time
		expectedContinue bool
	}{
		{
			desc: "infinite retries",
			settings: Settings{
				InitialBackoff: time.Second,
				Multiplier:     2,
			},
			expectedNext: []time.Time{
				startTime.Add(time.Second),
				startTime.Add(time.Second * 3),
				startTime.Add(time.Second * 7),
				startTime.Add(time.Second * 15),
			},
			expectedContinue: true,
		},
		{
			desc: "max backoff",
			settings: Settings{
				InitialBackoff: time.Second,
				Multiplier:     2,
				MaxBackoff:     time.Second * 2,
			},
			expectedNext: []time.Time{
				startTime.Add(time.Second),
				startTime.Add(time.Second * 3),
				startTime.Add(time.Second * 5),
				startTime.Add(time.Second * 7),
			},
			expectedContinue: true,
		},
		{
			desc: "max retries",
			settings: Settings{
				InitialBackoff: time.Second,
				Multiplier:     2,
				MaxRetries:     3,
			},
			expectedNext: []time.Time{
				startTime.Add(time.Second),
				startTime.Add(time.Second * 3),
				startTime.Add(time.Second * 7),
			},
			expectedContinue: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			r := mustRetryWithTime(t, startTime, tc.settings)
			for i, expectedNext := range tc.expectedNext {
				require.Equal(t, i+1, r.Iteration)
				require.Equal(t, r.NextRetry, expectedNext)
				if i < len(tc.expectedNext)-1 {
					require.True(t, r.ShouldContinue())
				}
				r.Next()
			}
			require.Equal(t, tc.expectedContinue, r.ShouldContinue())
		})
	}
}

func mustRetryWithTime(t *testing.T, ti time.Time, settings Settings) *Retry {
	ret, err := NewRetryWithTime(ti, settings)
	require.NoError(t, err)
	return ret
}
