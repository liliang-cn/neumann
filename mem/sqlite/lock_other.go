//go:build !unix

package sqlite

import "os"

// lockFile is a no-op on platforms without flock: single ownership is not
// enforced there, so callers must make sure only one Store opens a database.
func lockFile(path string) (*os.File, error) { return nil, nil }

func unlockFile(f *os.File) error { return nil }
