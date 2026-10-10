package observe

import (
	"bytes"
	"os"
	"strconv"
)

// residentBytes reads the resident set size from /proc/self/statm: one
// small read, no stop of the world.
func residentBytes() (int64, bool) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	f := bytes.Fields(b)
	if len(f) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(string(f[1]), 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * int64(os.Getpagesize()), true
}
