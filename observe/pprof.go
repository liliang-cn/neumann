package observe

import (
	"fmt"
	"html"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"
)

// pprofHandler serves the runtime profiles under /debug/pprof/ the way
// net/http/pprof does (go tool pprof reads them), without importing that
// package: its init registers the profiles on http.DefaultServeMux, and a
// host that serves DefaultServeMux would expose them without asking.
//
//	/debug/pprof/                index
//	/debug/pprof/<profile>       heap, goroutine, allocs, block, mutex, threadcreate; ?debug=N for text
//	/debug/pprof/profile         CPU profile, ?seconds=N (default 30)
//	/debug/pprof/trace           execution trace, ?seconds=N (default 1)
//	/debug/pprof/cmdline         the command line
func pprofHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/debug/pprof/")
	seconds := func(def int) time.Duration {
		if n, err := strconv.Atoi(r.URL.Query().Get("seconds")); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
		return time.Duration(def) * time.Second
	}
	sleep := func(d time.Duration) {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
		}
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	switch name {
	case "":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		profiles := pprof.Profiles()
		sort.Slice(profiles, func(i, j int) bool { return profiles[i].Name() < profiles[j].Name() })
		fmt.Fprint(w, "<html><body><h1>/debug/pprof/</h1><ul>")
		for _, p := range profiles {
			n := html.EscapeString(p.Name())
			fmt.Fprintf(w, `<li><a href="%s?debug=1">%s</a> (%d)</li>`, n, n, p.Count())
		}
		fmt.Fprint(w, `<li><a href="profile">profile</a> (CPU, ?seconds=30)</li><li><a href="trace">trace</a> (?seconds=1)</li></ul></body></html>`)
	case "cmdline":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, strings.Join(os.Args, "\x00"))
	case "profile":
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="profile"`)
		if err := pprof.StartCPUProfile(w); err != nil {
			http.Error(w, "cpu profile: "+err.Error(), http.StatusInternalServerError)
			return
		}
		sleep(seconds(30))
		pprof.StopCPUProfile()
	case "trace":
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="trace"`)
		if err := trace.Start(w); err != nil {
			http.Error(w, "trace: "+err.Error(), http.StatusInternalServerError)
			return
		}
		sleep(seconds(1))
		trace.Stop()
	default:
		p := pprof.Lookup(name)
		if p == nil {
			http.Error(w, "unknown profile", http.StatusNotFound)
			return
		}
		debug, _ := strconv.Atoi(r.URL.Query().Get("debug"))
		if debug > 0 {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		} else {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
		}
		if name == "heap" && r.URL.Query().Get("gc") != "" {
			runtime.GC()
		}
		p.WriteTo(w, debug)
	}
}
