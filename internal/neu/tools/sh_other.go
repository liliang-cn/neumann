//go:build !unix

package tools

import "os/exec"

// killGroup leaves cmd as it is: a timeout kills the command itself.
func killGroup(*exec.Cmd) {}
