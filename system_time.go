package blobs

import "time"

// Interface to specify the time of the system time right now.
type SystemTime interface {
	Now() time.Time
}

type realClock struct{}

func (c realClock) Now() time.Time {
	return time.Now()
}

// System time mock, useful for mocking the system time in tests.
type SystemTimeMock struct {
	Time time.Time
}

// Returns the mocked time.
func (c *SystemTimeMock) Now() time.Time {
	return c.Time
}
