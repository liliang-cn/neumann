//go:build unix

package sqlite

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking advisory lock on path (creating
// it if needed). The lock lives as long as the returned file is open. flock
// locks belong to the open file description, so a second Open in the same
// process conflicts just like one from another process.
func lockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, err
	}
	return f, nil
}

// unlockFile releases the lock by closing the file.
func unlockFile(f *os.File) error { return f.Close() }
