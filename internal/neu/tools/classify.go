package tools

import (
	"slices"
	"strings"
)

// ReadOnlyCommand reports whether an sh_run command line (argv mode, no
// shell) only reads: a short allowlist of programs, each with the
// arguments that would make it write, delete or run something else
// refused. It is a convenience that spares an approval prompt, not a
// security boundary: anything it does not recognise needs approval.
func ReadOnlyCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	for _, a := range args {
		if strings.ContainsRune(a, 0) {
			return false
		}
	}
	prog, rest := args[0], args[1:]
	if prog == "" || strings.ContainsAny(prog, `/\`) {
		return false // a path: whatever it runs is not on the list
	}
	switch prog {
	case "ls", "cat", "head", "tail", "wc", "grep", "egrep", "fgrep", "pwd", "stat", "du", "which", "basename", "dirname", "realpath":
		return true
	case "tree":
		return !hasFlag(rest, "o") // -o writes its output to a file
	case "rg":
		// --pre runs a program on every file, --hostname-bin one program.
		return !hasFlag(rest, "pre", "pre-glob", "hostname-bin")
	case "find":
		for _, a := range rest {
			switch a {
			case "-delete", "-exec", "-execdir", "-ok", "-okdir", "-fprint", "-fprint0", "-fprintf", "-fls":
				return false
			}
		}
		return true
	case "go":
		return readOnlyGo(rest)
	case "git":
		return readOnlyGit(rest)
	case "make":
		for _, a := range rest {
			switch a {
			case "-n", "--dry-run", "--just-print", "--recon":
				return true
			}
		}
		return false
	}
	return false
}

// hasFlag reports whether args hold any of the flags (named without
// dashes), as -name, --name or -name=value / --name=value.
func hasFlag(args []string, names ...string) bool {
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if slices.Contains(names, name) {
			return true
		}
	}
	return false
}

func readOnlyGo(args []string) bool {
	if len(args) == 0 {
		return false
	}
	sub, rest := args[0], args[1:]
	// Flags that run another program or write a file wherever they say.
	common := []string{"o", "exec", "toolexec", "overlay", "modfile", "pgo", "vettool"}
	switch sub {
	case "version", "list", "doc":
		return !hasFlag(rest, common...)
	case "env":
		return !hasFlag(rest, "w", "u")
	case "build", "vet":
		return !hasFlag(rest, common...)
	case "test":
		if hasFlag(rest, common...) || hasFlag(rest, "c", "outputdir") {
			return false
		}
		for _, a := range rest {
			name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
			if strings.HasPrefix(a, "-") && strings.HasSuffix(name, "profile") {
				return false // -coverprofile, -cpuprofile, ...: write a file
			}
		}
		return true
	}
	return false
}

// gitReadOnly are the git subcommands sh_run runs without asking: git_run's
// list and a few more that only read.
var gitReadOnly = []string{"log", "show", "diff", "status", "blame", "rev-parse", "ls-files", "grep", "describe", "shortlog", "cat-file", "ls-tree", "rev-list", "reflog"}

func readOnlyGit(args []string) bool {
	// args[0] must be the subcommand: a global option (-c, -C, --exec-path,
	// ...) could change what it runs.
	if len(args) == 0 || !slices.Contains(gitReadOnly, args[0]) {
		return false
	}
	if args[0] == "reflog" && len(args) > 1 && !strings.HasPrefix(args[1], "-") && args[1] != "show" {
		return false // reflog expire / delete
	}
	if args[0] == "grep" {
		for _, a := range args[1:] {
			if strings.HasPrefix(a, "-O") || strings.HasPrefix(a, "--open-files-in-pager") {
				return false // runs a pager program
			}
		}
	}
	return GitArgs(append([]string{"log"}, args[1:]...)) == nil
}

// gitSafe are the options sh_run puts before a read-only git command, as
// git_run does: no pager, no fsmonitor hook, no external diff.
var gitSafe = []string{"--no-pager", "-c", "core.fsmonitor=false", "-c", "diff.external=", "-c", "core.pager=cat"}
