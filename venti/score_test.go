package venti

import "testing"

func TestSha1Check(t *testing.T) {
	for _, test := range []struct {
		data  []byte
		score string
	}{
		{data: []byte{}, score: "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{data: []byte("test"), score: "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
		{data: []byte("foobar\n"), score: "988881adc9fc3655077dc2d4d757d480b5ea0e11"},
	} {
		parsed, err := ParseScore(test.score)
		if err != nil {
			t.Errorf("failed to parse score %s: %v", test.score, err)
			continue
		}
		if err := Sha1Check(parsed, test.data); err != nil {
			t.Errorf("Sha1Check %v %q: %v", test.score, test.data, err)
		}
	}
}
