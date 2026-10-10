package core

import (
	"context"
	"errors"
	"time"

	"github.com/liliang-cn/neumann/limit"
	"github.com/liliang-cn/neumann/model"
	"github.com/liliang-cn/neumann/proto"
	"github.com/liliang-cn/neumann/task"
)

// DefaultMaxOut is the output cap assumed for an adapter that does not say
// what it sends (model.OutputCap): what is reserved for output tokens, and
// what a truncated turn's cap is raised from.
const DefaultMaxOut = 4096

// throttleReport is how long a reservation may wait before the wait is
// reported (a throttled event): shorter waits are ordinary queueing.
const throttleReport = 50 * time.Millisecond

// maxOut is the task's own output cap: its MaxOut, else what the adapter
// sends by default, else DefaultMaxOut. A truncation redo raises from it.
func (e *Env) maxOut(t *task.TCB) int {
	if t.MaxOut > 0 {
		return t.MaxOut
	}
	if oc, ok := e.adapter(e.groupOf(t.Class)).(model.OutputCap); ok && oc.MaxOut() > 0 {
		return oc.MaxOut()
	}
	return DefaultMaxOut
}

// sendOut is the output cap t's next request carries: its own MaxOut (0:
// the adapter's default) or, after Truncations redos in a row, the task's
// cap raised fourfold that many times.
func (e *Env) sendOut(g *Group, t *task.TCB) int {
	if t.Truncations == 0 {
		return t.MaxOut
	}
	return e.raisedOut(g, t, t.Truncations)
}

// raisedOut is the task's cap raised fourfold n times, but never above what
// the group's limiters could ever grant one request in output: a raise past
// that would make the request too large to send at all.
func (e *Env) raisedOut(g *Group, t *task.TCB, n int) int {
	v := int64(e.maxOut(t))
	for range n {
		v *= 4
	}
	if c := outCeiling(g); c > 0 && v > c {
		v = c
	}
	return int(v)
}

// outCeiling is the smallest Limit of an output_tokens or tokens dimension
// among the group's limiters; 0 when none has one.
func outCeiling(g *Group) int64 {
	if g == nil {
		return 0
	}
	var ceil int64
	for _, l := range g.Limiters {
		for _, dim := range []string{model.DimOutputTokens, model.DimTokens} {
			if d, ok := l.Status().Dims[dim]; ok && (ceil == 0 || d.Limit < ceil) {
				ceil = d.Limit
			}
		}
	}
	return ceil
}

// reserveAmounts is what a request takes from its limiters before it is
// sent: the adapter's own figures when it is model.Metered, otherwise one
// request, an estimate of the input tokens for "tokens" and "input_tokens"
// (the request's JSON bytes / 4: rough, but cheap, and never zero for a
// large context), and the output cap for "output_tokens". Settlement corrects
// the estimate with what the provider reported.
func reserveAmounts(a model.Adapter, req model.Request, out int) map[string]int64 {
	if m, ok := a.(model.Metered); ok {
		return m.Reserve(req)
	}
	in := estimateInput(req)
	return map[string]int64{
		model.DimRequests:     1,
		model.DimTokens:       in,
		model.DimInputTokens:  in,
		model.DimOutputTokens: int64(out),
	}
}

func estimateInput(req model.Request) int64 {
	return task.EstimateTokens(req.System, req.Tools, req.Messages)
}

// settleAmounts is what a request used, from its response: the adapter's own
// figures when it is model.Metered, otherwise the reported usage. A response
// that reported no usage settles only the request: its tokens stay at the
// reservation, since nothing better is known.
func settleAmounts(a model.Adapter, req model.Request, resp model.Response) map[string]int64 {
	if m, ok := a.(model.Metered); ok {
		return m.Settle(req, resp)
	}
	out := map[string]int64{model.DimRequests: 1}
	u := resp.Usage
	if u.PromptTokens == 0 && u.CompletionTokens == 0 {
		return out
	}
	out[model.DimTokens] = int64(u.PromptTokens + u.CompletionTokens)
	out[model.DimInputTokens] = int64(u.PromptTokens)
	out[model.DimOutputTokens] = int64(u.CompletionTokens)
	return out
}

// reserve takes room for one request in every limiter of its group, waiting
// as long as it takes (design §7.2). It runs before the request's Running
// commit: a throttled task changes no state. ctx is the task's: a cancel,
// its deadline or a halt ends the wait with nothing reserved. A wait longer
// than throttleReport is returned as a throttled event, for the next commit.
func (e *Env) reserve(ctx context.Context, g *Group, taskID string, req model.Request, out int) (*limit.Reservation, []proto.Event, error) {
	if g == nil || len(g.Limiters) == 0 {
		return nil, nil, nil
	}
	var amounts map[string]int64
	if err := guarded("Reserve", func() { amounts = reserveAmounts(e.adapter(g), req, out) }); err != nil {
		return nil, nil, err
	}
	start := e.Clock.Now()
	res, err := limit.ReserveAll(ctx, g.Limiters, amounts)
	if err != nil {
		return nil, nil, err
	}
	var evs []proto.Event
	if waited := e.Clock.Now().Sub(start); waited > throttleReport {
		evs = append(evs, proto.NewEvent(proto.EvThrottled, "", map[string]any{
			"task": taskID, "class": g.Class, "waited_ms": waited.Milliseconds(),
		}))
	}
	return res, evs, nil
}

// refund is what becomes of a reservation once its call is over.
type refund int

const (
	settleActual   refund = iota // answered: the usage it reported
	settleReserved               // failed, but the provider may have counted it
	cancelAll                    // never counted: everything goes back
)

// refundOf decides a call's refund (design §7.2):
//
//   - a response settles at what it used;
//   - rate limited and overloaded answers, a lost response, a transient
//     failure with a status (a 5xx) and a call cut off by a cancel all
//     reached the provider, which may have counted them: they keep the
//     reservation;
//   - a transient failure with no status that did not lose a response (a
//     refused or failed connection: the request never got out), a permanent
//     rejection, an exhausted quota, and a panic in the adapter give it all
//     back.
func refundOf(err error, class model.ErrClass) refund {
	if err == nil {
		return settleActual
	}
	switch class {
	case model.RateLimited, model.Overload, model.Canceled:
		return settleReserved
	case model.Transient:
		var me *model.Error
		if errors.Is(err, model.ErrResponseLost) || (errors.As(err, &me) && me.Status != 0) {
			return settleReserved
		}
	}
	return cancelAll
}

// settleCall ends a reservation after its call: settled first, then calibrated
// with what the provider said (design §7.3). The order matters: the
// provider's remaining figure already counts this request, so observing it
// before the settle would charge the request's extra usage twice. A panic
// in the adapter's Settle gives the reservation back and is returned.
func (e *Env) settleCall(g *Group, res *limit.Reservation, how refund, req model.Request, resp model.Response, info *model.RateInfo) error {
	if res != nil {
		switch how {
		case settleActual:
			var actual map[string]int64
			if err := guarded("Settle", func() { actual = settleAmounts(e.adapter(g), req, resp) }); err != nil {
				res.Cancel()
				return err
			}
			res.Settle(actual)
		case settleReserved:
			res.Settle(nil)
		default:
			res.Cancel()
		}
	}
	if g != nil && info != nil {
		obs := limit.Info{Dims: make(map[string]limit.Observed, len(info.Dims))}
		for name, d := range info.Dims {
			obs.Dims[name] = limit.Observed{Remaining: d.Remaining, ResetAt: d.ResetAt}
		}
		for _, l := range g.Limiters {
			l.Observe(obs)
		}
	}
	return nil
}

// rateInfo is what the provider said about its limits on this call: the
// adapter's model.RateReporter when it has one, else what the response or
// the error carries.
func rateInfo(a model.Adapter, resp model.Response, err error) *model.RateInfo {
	if rr, ok := a.(model.RateReporter); ok {
		if err == nil {
			return rr.RateInfo(&resp, nil)
		}
		return rr.RateInfo(nil, err)
	}
	if err == nil {
		return resp.Rate
	}
	var me *model.Error
	if errors.As(err, &me) {
		return me.Rate
	}
	return nil
}

// retryAt is when a rate-limited request may be sent again, as the provider
// said: its retry-after, else the instant its rate info gives, else the
// latest reset of a dimension it reported exhausted. ok is false when it
// said none of these.
func (e *Env) retryAt(err error, info *model.RateInfo) (at time.Time, ok bool) {
	now := e.Clock.Now()
	var me *model.Error
	if errors.As(err, &me) && me.RetryAfter > 0 {
		return now.Add(me.RetryAfter), true
	}
	if info != nil {
		if info.RetryAt.After(now) {
			return info.RetryAt, true
		}
		var at time.Time
		for _, d := range info.Dims {
			if d.Remaining <= 0 && d.ResetAt.After(at) {
				at = d.ResetAt
			}
		}
		if at.After(now) {
			return at, true
		}
	}
	return time.Time{}, false
}

// pause stops every limiter of the group granting anything before at: the
// other tasks of the group wait too, instead of each earning its own 429.
func pause(g *Group, at time.Time) {
	if g == nil {
		return
	}
	for _, l := range g.Limiters {
		l.PauseUntil(at)
	}
}
