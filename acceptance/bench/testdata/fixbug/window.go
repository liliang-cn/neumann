// Package window computes statistics over sliding windows of a series.
package window

// Sums returns the sum of every run of w consecutive values of xs, in
// order: len(xs)-w+1 sums when 1 <= w <= len(xs), nil otherwise.
func Sums(xs []int, w int) []int {
	if w <= 0 || w > len(xs) {
		return nil
	}
	out := make([]int, 0, len(xs)-w+1)
	sum := 0
	for i := 0; i < w; i++ {
		sum += xs[i]
	}
	out = append(out, sum)
	for i := w; i < len(xs)-1; i++ {
		sum += xs[i] - xs[i-w]
		out = append(out, sum)
	}
	return out
}

// MaxSum returns the largest window sum and the index of the first value
// of that window; ok is false when there is no window.
func MaxSum(xs []int, w int) (best, at int, ok bool) {
	sums := Sums(xs, w)
	if len(sums) == 0 {
		return 0, 0, false
	}
	best = sums[0]
	for i, s := range sums {
		if s > best {
			best, at = s, i
		}
	}
	return best, at, true
}

// Average returns the mean of each window, as Sums divided by w.
func Average(xs []int, w int) []float64 {
	sums := Sums(xs, w)
	out := make([]float64, len(sums))
	for i, s := range sums {
		out[i] = float64(s) / float64(w)
	}
	return out
}
