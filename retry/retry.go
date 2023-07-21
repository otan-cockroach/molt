package retry

import (
	"math"
	"time"

	"github.com/cockroachdb/errors"
)

type Settings struct {
	InitialBackoff time.Duration
	Multiplier     int
	MaxBackoff     time.Duration
	MaxRetries     int
}

func (s Settings) Verify() error {
	if s.InitialBackoff <= 0 {
		return errors.Newf("initial backoff must be set to >= 0, got %s", s.InitialBackoff)
	}
	if s.Multiplier < 1 {
		return errors.Newf("multiplier must be >= 1, got %d", s.Multiplier)
	}
	if s.MaxBackoff > 0 && s.InitialBackoff > s.MaxBackoff {
		return errors.Newf("initial backoff (%s) must be less than max backoff (%s)", s.InitialBackoff, s.MaxBackoff)
	}
	return nil
}

func DefaultSettings() Settings {
	return Settings{
		InitialBackoff: time.Second,
		Multiplier:     2,
	}
}

type Retry struct {
	Iteration int
	StartTime time.Time
	NextRetry time.Time

	settings Settings
}

func NewRetry(settings Settings) (*Retry, error) {
	return NewRetryWithTime(time.Now(), settings)
}

func NewRetryWithTime(t time.Time, settings Settings) (*Retry, error) {
	if err := settings.Verify(); err != nil {
		return nil, err
	}
	return &Retry{
		Iteration: 1,
		StartTime: t,
		NextRetry: t.Add(settings.InitialBackoff),
		settings:  settings,
	}, nil
}

func (rm *Retry) ShouldContinue() bool {
	if rm.settings.MaxRetries == 0 {
		return true
	}
	return rm.Iteration < rm.settings.MaxRetries
}

func (rm *Retry) Next() {
	nextDuration := rm.settings.InitialBackoff * time.Duration(math.Pow(float64(rm.settings.Multiplier), float64(rm.Iteration)))
	if rm.settings.MaxBackoff > 0 && nextDuration > rm.settings.MaxBackoff {
		nextDuration = rm.settings.MaxBackoff
	}
	rm.Iteration++
	rm.NextRetry = rm.NextRetry.Add(nextDuration)
}
