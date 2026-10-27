// Command bench is neu's benchmark harness: a suite of realistic tasks with
// objective checkers, run against a real backend in several scenarios
// (solo, mixed, ops, crash, burst), measured from the machine's committed
// event stream. It is a development tool, not part of the neu binary.
//
//	go run ./acceptance/bench -scenario solo -out report.json
//	go run ./acceptance/bench -help
//
// Run it from the repository root (or pass -repo). The openai backend's
// defaults are CPA_BASE_URL, CPA_MODEL and the key in CPA_API_KEY.
package main

import (
	"context"
	"embed"
	"os"
	"os/signal"
	"syscall"
)

// testdata is the bench's committed fixtures.
//
//go:embed testdata
var testdata embed.FS

func main() {
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(Main(ctx, os.Args[1:], testdata, os.Stdout, os.Stderr))
}
