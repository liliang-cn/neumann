//go:build !unix

package mcp

import "os/exec"

func ownGroup(*exec.Cmd) {}

func killGroup(int) {}
