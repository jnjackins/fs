package main

import "testing"

func TestTokenize(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"the quick brown fox", []string{"the", "quick", "brown", "fox"}},
		{"'the quick' brown fox", []string{"the quick", "brown", "fox"}},
		{"the quick brown' fox'", []string{"the", "quick", "brown fox"}},
		{"	the   quick bro'wn 'fox", []string{"the", "quick", "brown fox"}},
		{"'the quick ' brown fox      ", []string{"the quick ", "brown", "fox"}},
	}

	for _, test := range tests {
		out := tokenize(test.input)
		if len(out) != len(test.want) {
			t.Errorf("tokenize(%s)=%v, want=%v", test.input, out, test.want)
			continue
		}
		for i := range test.want {
			if out[i] != test.want[i] {
				t.Errorf("tokenize(%s)=%v, want=%v", test.input, out, test.want)
				continue
			}
		}
	}
}
