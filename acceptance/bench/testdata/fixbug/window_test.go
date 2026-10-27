package window

import (
	"slices"
	"testing"
)

func TestSums(t *testing.T) {
	cases := []struct {
		xs   []int
		w    int
		want []int
	}{
		{[]int{1, 2, 3, 4, 5}, 2, []int{3, 5, 7, 9}},
		{[]int{1, 2, 3, 4, 5}, 5, []int{15}},
		{[]int{4, -1, 7}, 1, []int{4, -1, 7}},
		{[]int{1, 2, 3}, 4, nil},
		{[]int{1, 2, 3}, 0, nil},
	}
	for _, c := range cases {
		if got := Sums(c.xs, c.w); !slices.Equal(got, c.want) {
			t.Errorf("Sums(%v, %d) = %v, want %v", c.xs, c.w, got, c.want)
		}
	}
}

func TestMaxSum(t *testing.T) {
	best, at, ok := MaxSum([]int{2, 1, 0, 3, 9}, 2)
	if !ok || best != 12 || at != 3 {
		t.Errorf("MaxSum = %d at %d (%v), want 12 at 3", best, at, ok)
	}
}

func TestAverage(t *testing.T) {
	got := Average([]int{2, 4, 6, 8}, 2)
	want := []float64{3, 5, 7}
	if !slices.Equal(got, want) {
		t.Errorf("Average = %v, want %v", got, want)
	}
}
