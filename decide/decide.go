package decide

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Type is the kind of a question.
type Type string

const (
	TypeChoice Type = "choice" // which one of Options fits
	TypeScore  Type = "score"  // where on the scale of Options, lowest first
	TypeNoul   Type = "noul"   // yes or no
)

// MaxOptions is the most options a choice, or levels a score, may have;
// both need at least two. System One refuses anything outside 2 to 26.
const MaxOptions = 26

// MaxQuestions is the most questions one request may ask; System One
// refuses more.
const MaxQuestions = 64

// Question is one closed question about a text. Build one with Choice, Score
// or Noul, and add descriptions with Describe.
type Question struct {
	Type Type
	// Instructions is the question in plain words, addressed to the engine.
	Instructions string
	// Options are a choice's options, in order, or a score's levels, lowest
	// first. A noul has none. Order matters: an engine breaks ties by it.
	Options []string
	// Descriptions say in one line what an option of a choice means, or,
	// for a noul, what "yes" and what "no" mean. Optional, but a described
	// option is answered far more reliably than a bare label. A score takes
	// none: its levels are their own descriptions.
	Descriptions map[string]string
}

// Choice asks which of options fits the text.
func Choice(instructions string, options ...string) Question {
	return Question{Type: TypeChoice, Instructions: instructions, Options: slices.Clone(options)}
}

// Score asks where the text sits on an ordered scale, lowest level first.
func Score(instructions string, levels ...string) Question {
	return Question{Type: TypeScore, Instructions: instructions, Options: slices.Clone(levels)}
}

// Noul asks a yes-or-no question.
func Noul(instructions string) Question {
	return Question{Type: TypeNoul, Instructions: instructions}
}

// Describe returns q with option (for a noul, "yes" or "no") described as
// text. q itself is not changed.
func (q Question) Describe(option, text string) Question {
	q.Descriptions = maps.Clone(q.Descriptions)
	if q.Descriptions == nil {
		q.Descriptions = map[string]string{}
	}
	q.Descriptions[option] = text
	return q
}

// Validate reports whether q can be asked as written.
func (q Question) Validate() error {
	if strings.TrimSpace(q.Instructions) == "" {
		return fmt.Errorf("decide: a %s question has no instructions", q.Type)
	}
	switch q.Type {
	case TypeNoul:
		if len(q.Options) > 0 {
			return fmt.Errorf("decide: a noul has no options")
		}
		for k := range q.Descriptions {
			if k != "yes" && k != "no" {
				return fmt.Errorf("decide: a noul describes yes and no, not %q", k)
			}
		}
		return nil
	case TypeChoice, TypeScore:
		if len(q.Options) < 2 || len(q.Options) > MaxOptions {
			return fmt.Errorf("decide: a %s needs 2 to %d options, not %d", q.Type, MaxOptions, len(q.Options))
		}
		seen := map[string]bool{}
		for _, o := range q.Options {
			if strings.TrimSpace(o) == "" {
				return fmt.Errorf("decide: a %s has an empty option", q.Type)
			}
			if seen[o] {
				return fmt.Errorf("decide: a %s lists %q twice", q.Type, o)
			}
			seen[o] = true
		}
		if q.Type == TypeScore && len(q.Descriptions) > 0 {
			return fmt.Errorf("decide: a score's levels are their own descriptions")
		}
		for k := range q.Descriptions {
			if !seen[k] {
				return fmt.Errorf("decide: %q is described but is not an option", k)
			}
		}
		return nil
	default:
		return fmt.Errorf("decide: unknown question type %q", q.Type)
	}
}

// Ask is one question and the name its answer comes back under.
type Ask struct {
	Name     string
	Question Question
}

// Validate checks a request's questions before it is sent: 1 to
// MaxQuestions of them, each named, no name twice, and each valid.
func Validate(asks []Ask) error {
	if len(asks) == 0 || len(asks) > MaxQuestions {
		return fmt.Errorf("decide: a request asks 1 to %d questions, not %d", MaxQuestions, len(asks))
	}
	seen := make(map[string]bool, len(asks))
	for _, a := range asks {
		if strings.TrimSpace(a.Name) == "" {
			return fmt.Errorf("decide: a question has no name")
		}
		if seen[a.Name] {
			return fmt.Errorf("decide: two questions are named %q", a.Name)
		}
		seen[a.Name] = true
		if err := a.Question.Validate(); err != nil {
			return fmt.Errorf("%w (question %q)", err, a.Name)
		}
	}
	return nil
}

// Answer is an engine's answer to one question.
type Answer struct {
	Type Type
	// Label is the answer whatever the type: the option chosen, "yes" or
	// "no", or for a score the level with the highest probability (the
	// lower one on a tie), which need not be the level nearest Score.
	Label string
	// Yes is a noul's verdict: its probability of yes is at least 0.5.
	// False for other types.
	Yes bool
	// Confidence is how sure the engine is of Label, in [0, 1]. For a choice
	// and a score it is the engine's own calibrated number, which is lower
	// than the winning probability. A noul comes with only its probability p
	// of yes, so its confidence is |2p - 1|: 0 for a coin toss, 1 for a
	// certain answer either way.
	Confidence float64
	// Score is a score's position on its scale: the expected level index
	// under Probabilities, from 0 (the first level) to len(Options)-1, not
	// rounded. A spread distribution puts it between levels, away from
	// Label. 0 for other types.
	Score float64
	// Probabilities is the whole distribution: by option for a choice, by
	// level for a score, "yes" and "no" for a noul.
	Probabilities map[string]float64
}

// P is the probability the engine gave label (an option, a level, "yes" or
// "no"); 0 if it gave none.
func (a Answer) P(label string) float64 { return a.Probabilities[label] }

// Engine answers closed questions about a text.
//
// Decide answers every question in one request, keyed by Ask.Name: an engine
// reads the text once whatever the number of questions, so asking them
// together costs about what one does. The questions are asked in the order
// given, and the order can change the answers: System One answers them one
// after another, each in sight of the answers before it. A question asked
// after a related one is often answered better than the same question
// alone, so put the question the others depend on first.
//
// An error means no answer at all; callers degrade, they never guess.
// Decide must return promptly, with an error, once ctx is done: Policy,
// Router and Checker stop waiting for it then whatever it does, but a
// Decide that ignores ctx keeps its goroutine. Implementations must be safe
// for concurrent use.
type Engine interface {
	// Name says which engine and model answered, for reasons and logs.
	Name() string
	Decide(ctx context.Context, state string, asks []Ask) (map[string]Answer, error)
}
