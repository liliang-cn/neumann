package mem

import "time"

// CommitTime is the time a store stamps on the events of a commit set: now
// in UTC, to the nanosecond, without a monotonic reading, and never before
// last, the time of the set committed before it. A wall clock that steps
// back therefore cannot make the event log go back in time; the events
// stay at the last time until the clock catches up.
func CommitTime(now, last time.Time) time.Time {
	t := time.Unix(0, now.UnixNano()).UTC()
	if t.Before(last) {
		return last
	}
	return t
}
