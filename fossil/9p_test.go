package main

import "testing"

func TestParseAname(t *testing.T) {
	testCases := []struct {
		aname, fsname, path string
	}{
		{"", "main", "active"},
		{"main/active", "main", "active"},
		{"fsname", "fsname", ""},
	}

	for _, c := range testCases {
		fs, path := parseAname(c.aname)
		if fs != c.fsname || path != c.path {
			t.Errorf("%q: got fsname=%q path=%q, wanted fsname=%q path=%q ",
				fs, path, c.fsname, c.path)
		}
	}
}
