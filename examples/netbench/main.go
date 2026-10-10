// Command netbench measures what the wire protocol costs against a real
// OpenAI-compatible endpoint, separately from what the model costs: a cold
// connection, warm round trips without inference (GET /models), a burst of
// concurrent round trips, and a few tiny completions.
//
//	OPENAI_BASE_URL=... OPENAI_API_KEY=... OPENAI_MODEL=... go run ./examples/netbench
//
// LLM_BASE_URL / LLM_API_KEY / LLM_MODEL are used when the OPENAI_ ones are unset.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/model/openai"
	"github.com/liliang-cn/neumann/task"
)

func env(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func get(ctx context.Context, a *openai.Adapter) (time.Duration, string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, a.BaseURL+"/models", nil)
	req.Header.Set("Authorization", "Bearer "+a.APIKey)
	start := time.Now()
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return 0, "", err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return time.Since(start), resp.Proto, nil
}

func stats(ds []time.Duration) string {
	slices.Sort(ds)
	p := func(q float64) time.Duration { return ds[int(q*float64(len(ds)-1))] }
	return fmt.Sprintf("p50 %4dms  p90 %4dms  min %4dms", p(0.5).Milliseconds(), p(0.9).Milliseconds(), ds[0].Milliseconds())
}

func main() {
	base, key, modelName := env("OPENAI_BASE_URL", "LLM_BASE_URL"), env("OPENAI_API_KEY", "LLM_API_KEY"), env("OPENAI_MODEL", "LLM_MODEL")
	if base == "" || key == "" || modelName == "" {
		log.Fatal("set OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL (or the LLM_ equivalents)")
	}
	ctx := context.Background()
	const rounds, burst, completions = 20, 16, 3
	fmt.Printf("endpoint %s, model %s\n\n", base, modelName)
	for _, proto := range []openai.Protocol{openai.HTTP1, openai.HTTP2, openai.HTTP3} {
		a := openai.New(base, key, modelName).WithProtocol(proto)

		cold, wire, err := get(ctx, a)
		if err != nil {
			fmt.Printf("%-9s cold connect failed: %v\n\n", proto, err)
			continue
		}

		var warm []time.Duration
		for range rounds {
			d, _, err := get(ctx, a)
			if err != nil {
				log.Fatal(err)
			}
			warm = append(warm, d)
		}

		var mu sync.Mutex
		var each []time.Duration
		var wg sync.WaitGroup
		bstart := time.Now()
		for range burst {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, _, err := get(ctx, a)
				if err != nil {
					log.Print(err)
					return
				}
				mu.Lock()
				each = append(each, d)
				mu.Unlock()
			}()
		}
		wg.Wait()
		bwall := time.Since(bstart)

		var comp []time.Duration
		for range completions {
			s := time.Now()
			_, err := a.Call(ctx, model.Request{Messages: []task.Message{task.Text(task.RoleUser, "Reply with the single digit 1.")}})
			if err != nil {
				log.Fatal(err)
			}
			comp = append(comp, time.Since(s))
		}

		fmt.Printf("%-9s served as %s\n", proto, wire)
		fmt.Printf("  cold connect + first GET   %4dms\n", cold.Milliseconds())
		fmt.Printf("  warm GET x%d               %s\n", rounds, stats(warm))
		fmt.Printf("  burst of %d concurrent GET  wall %4dms   each %s\n", burst, bwall.Milliseconds(), stats(each))
		fmt.Printf("  tiny completion x%d         %s\n\n", completions, stats(comp))
		a.HTTP.CloseIdleConnections()
	}
}
