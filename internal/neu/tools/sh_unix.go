//go:build unix

package tools

import (
	"os/exec"
	"syscall"
)

// killGroup runs cmd in a process group of its own and makes a timeout or
// cancel kill the whole group, so a command's children die with it.
func killGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
