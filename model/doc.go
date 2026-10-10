// Package model is the seam between the machine and a model provider. Every
// call through an [Adapter] is one instruction on a vCore.
//
// # Getting started
//
// Use a ready-made adapter: [github.com/liliang-cn/neumann/model/openai] for
// any OpenAI-compatible /chat/completions endpoint, or
// [github.com/liliang-cn/neumann/model/anthropic] for Anthropic's Messages
// API. For tests and offline programs, [github.com/liliang-cn/neumann/sim]
// has a scripted Model.
//
// # Writing an adapter
//
// The required part is small: Provider names the provider (and model) for
// events and for deciding whose reasoning blocks a request may carry, and
// Call turns a provider-neutral [Request] into a [Response]. Tool calls come
// back as task.PartToolCall parts with ProviderCallID set.
//
// Everything else is optional, discovered by type assertion:
//
//   - [Streamer]: report text as it is generated; the machine streams
//     whenever an adapter implements it (Machine.Deltas). A wrapper that
//     forwards only Call silently turns streaming off.
//   - [CapsReporter] and [OutputCap]: what the model can take (context window,
//     output cap, tools), read before a task migrates onto its group.
//   - [Classifier]: map the provider's errors to an [ErrClass] better than
//     [Classify] can.
//   - [Metered]: how the provider counts a request against its rate limits
//     (reserve before, settle after); without it the machine estimates.
//   - [RateReporter]: read rate-limit headers, so limiters can calibrate.
//   - [Warmer]: open the connection at Start.
//
// Failures decide what the machine does next (design §11), so classify them:
// return a *[Error] with a Class for every failure you can explain, wrap
// [ErrResponseLost] when the request reached the provider but the answer did
// not arrive whole, and report a cancelled caller's ctx.Err() as is.
package model
