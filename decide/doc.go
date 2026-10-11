// Package decide asks decision engines closed questions: small models that
// answer typed questions about a text (which of these, how much, yes or no)
// in one forward pass, without generating anything.
//
// In the machine's terms a decision engine is a coprocessor. The vCores run
// instructions, each one a model call that is scheduled, metered against a
// group's window and limiters, and committed as a step of its task. A
// decision engine runs none of that: the code that asks it (a tool policy, a
// host routing a submission or checking an answer) waits for the answer in
// place, and the answer feeds a decision that code was making anyway. Its
// calls are not instructions: they are not task steps, they do not count
// against Budget.MaxSteps, no limiter or window sees them, and no event
// records them. The audit trail is the decision they lead to: Policy writes
// the engine's answer into the Reason of a held call, and that Reason is
// committed with the call record (design §9.7), so it survives a restart and
// an operator sees it in the approval_needed event.
//
// The pieces:
//
//   - Engine is the interface: one request answers every question about one
//     text. SystemOne is the client for Ollama's System One API (POST
//     /v1/systemone, Ollama 0.35 and later), which serves tev1:4b among
//     others. Package decidetest has a scripted engine for tests.
//   - Policy builds a neumann.ToolPolicy on an engine: it asks whether a
//     tool call needs a person, and holds it for approval when the engine
//     says yes.
//   - Router picks the class (execution group) a submission runs in: a
//     cheap model for easy requests, a strong one for hard ones.
//   - Checker reads a finished task's answer next to its request and says
//     whether it answers it, or only plans, or refuses; the host decides
//     what to do about a failed check.
//
// A host wires them up like this:
//
//	engine := decide.NewSystemOne(decide.SystemOneConfig{}) // tev1:4b on the local Ollama
//	cfg.ToolPolicy = decide.Policy(engine, decide.PolicyConfig{})
//
//	router, err := decide.NewRouter(engine, decide.RouterConfig{
//		Routes: []decide.Route{
//			{Class: "fast", Description: "short factual answers, one-step requests"},
//			{Class: "strong", Description: "multi-step reasoning, analysis, writing"},
//		},
//		Default: "strong",
//	})
//	class, _, _ := router.Route(ctx, input) // the default class if the engine fails
//	id, err := m.Submit(ctx, proto.Instruction{Input: input, Class: class})
//
// An engine's error means it gave no answer. Callers degrade to a fixed
// choice (Policy's Fallback, Router's Default, a Checker's pass); they never
// guess an answer.
//
// examples/decide runs all three offline, or against a local Ollama with
// -live.
package decide
