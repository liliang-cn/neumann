//go:build unix

package mcp

import (
	"os/exec"
	"syscall"
)

// ownGroup starts the server as the leader of its own process group, so that
// what it spawns can be found and stopped with it.
func ownGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killGroup kills whatever is left in the server's process group once the
// server itself has exited and been reaped.
func killGroup(pid int) {
	if pid > 0 {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}
