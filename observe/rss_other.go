//go:build !linux

package observe

// residentBytes is not read where it is not cheap: neumann.process.rss is
// left out.
func residentBytes() (int64, bool) { return 0, false }
