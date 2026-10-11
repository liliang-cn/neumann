package decide_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/liliang-cn/neumann/decide"
)

func TestQuestionValidate(t *testing.T) {
	many := make([]string, decide.MaxOptions+1)
	for i := range many {
		many[i] = fmt.Sprint("o", i)
	}
	ok := []decide.Question{
		decide.Noul("?"),
		decide.Noul("?").Describe("yes", "y").Describe("no", "n"),
		decide.Choice("?", "a", "b").Describe("a", "first"),
		decide.Choice("?", many[:decide.MaxOptions]...),
		decide.Score("?", "low", "high"),
	}
	for _, q := range ok {
		if err := q.Validate(); err != nil {
			t.Errorf("%+v: %v", q, err)
		}
	}
	bad := map[string]decide.Question{
		"no instructions":      decide.Noul(" "),
		"noul with options":    {Type: decide.TypeNoul, Instructions: "?", Options: []string{"a", "b"}},
		"noul described oddly": decide.Noul("?").Describe("maybe", "m"),
		"one option":           decide.Choice("?", "a"),
		"too many options":     decide.Choice("?", many...),
		"empty option":         decide.Choice("?", "a", " "),
		"repeated option":      decide.Choice("?", "a", "a"),
		"undescribed key":      decide.Choice("?", "a", "b").Describe("c", "?"),
		"described score":      decide.Score("?", "low", "high").Describe("low", "l"),
		"unknown type":         {Type: "rank", Instructions: "?"},
	}
	for name, q := range bad {
		if err := q.Validate(); err == nil || !strings.HasPrefix(err.Error(), "decide: ") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if err := decide.Validate(nil); err == nil {
		t.Error("no questions validated")
	}
	if err := decide.Validate(one("", decide.Noul("?"))); err == nil {
		t.Error("an unnamed question validated")
	}
	if err := decide.Validate(one("x", decide.Choice("?", "a"))); err == nil || !strings.Contains(err.Error(), `"x"`) {
		t.Errorf("err = %v", err)
	}
	twice := append(one("x", decide.Noul("?")), one("x", decide.Noul("!"))...)
	if err := decide.Validate(twice); err == nil || !strings.Contains(err.Error(), "two questions are named") {
		t.Errorf("err = %v", err)
	}
	var asks []decide.Ask
	for i := range decide.MaxQuestions {
		asks = append(asks, decide.Ask{Name: fmt.Sprint("q", i), Question: decide.Noul("?")})
	}
	if err := decide.Validate(asks); err != nil {
		t.Errorf("%d questions: %v", len(asks), err)
	}
	asks = append(asks, decide.Ask{Name: "one too many", Question: decide.Noul("?")})
	if err := decide.Validate(asks); err == nil || !strings.Contains(err.Error(), "1 to 64 questions, not 65") {
		t.Errorf("err = %v", err)
	}
}

func TestDescribeDoesNotShare(t *testing.T) {
	base := decide.Choice("?", "a", "b").Describe("a", "first")
	other := base.Describe("b", "second")
	if len(base.Descriptions) != 1 || len(other.Descriptions) != 2 {
		t.Fatalf("base %v, other %v", base.Descriptions, other.Descriptions)
	}
	opts := []string{"a", "b"}
	q := decide.Choice("?", opts...)
	opts[0] = "z"
	if q.Options[0] != "a" {
		t.Fatal("Choice keeps the caller's slice")
	}
}
