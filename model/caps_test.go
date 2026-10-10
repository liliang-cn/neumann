package model_test

import (
	"context"
	"testing"

	"github.com/liliang-cn/neumann/model"
)

type bare struct{}

func (bare) Provider() string { return "bare" }
func (bare) Call(context.Context, model.Request) (model.Response, error) {
	return model.Response{}, nil
}

type capped struct{ bare }

func (capped) MaxOut() int { return 777 }

// An adapter that reports nothing takes tools, any number of them, and is
// never refused for its window; its output cap comes from OutputCap.
func TestCapsOfDefaults(t *testing.T) {
	if got := model.CapsOf(bare{}); got != (model.Caps{Tools: true}) {
		t.Fatalf("bare %+v", got)
	}
	if got := model.CapsOf(capped{}); got != (model.Caps{Tools: true, MaxOut: 777}) {
		t.Fatalf("capped %+v", got)
	}
}
