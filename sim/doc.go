// Package sim holds deterministic stand-ins for the world outside the machine,
// for tests and offline examples.
//
// [Model] is a scripted model adapter: Fn sees each request (system prompt,
// tools, every message so far) and returns the next turn, so a script can
// branch on what has happened, for example by [ToolRounds]. [Reply] and
// [CallTools] build turns; [RateLimited], [Overload], [Transient],
// [Permanent], [QuotaExhausted], [Lost] and [Truncated] script the failures a
// real provider produces, already classified as an adapter would. Model also
// implements model.Streamer, so the streaming path is exercised.
//
//	m, err := neumann.New(neumann.Config{Model: &sim.Model{Fn: func(req model.Request) (model.Response, error) {
//		if sim.ToolRounds(req) == 0 {
//			return sim.CallTools("calc_add"), nil
//		}
//		return sim.Reply("done"), nil
//	}}})
//
// [Clock] is a fake clock that also runs timers. Pass it as
// neumann.Config.Clock to drive backoffs, deadlines, breakers and limiter
// waits by hand with Advance.
//
// examples/hello runs a whole task on a sim.Model.
package sim
