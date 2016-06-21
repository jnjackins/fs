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
				c.aname, fs, path, c.fsname, c.path)
		}
	}
}

func Test9p(t *testing.T) {
	err := cmd9p(nil, []string{"9p", "Tversion", "8192", "9P2000"})
	if err != nil {
		t.Fatal(err)
	}
	// err = cmd9p([]string{"9p", "Tattach", "0", "4294967295", "nobody", "main/active"})
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
