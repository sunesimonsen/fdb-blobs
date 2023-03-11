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
