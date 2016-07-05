package main

import (
	"strings"
	"testing"
)

func TestUserFile(t *testing.T) {
	if err := fsysConfig(nil, "main", []string{"config", testFossilPath}); err != nil {
		t.Fatalf("config fsys: %v", err)
	}

	if err := fsysOpen(nil, "main", []string{"open", "-AWPV"}); err != nil {
		t.Fatalf("open fsys: %v", err)
	}

	for _, cmd := range []string{
		"fsys main create /active/usr adm adm d775",
		"uname test test",
		"uname test1 :test1",
		"uname test1 %test2",
		"uname test3 test4",
		"uname test3 =glenda",
		"uname test2 =test2",
		"uname test2 =",
		"uname test +glenda",
		"uname test +test2",
		"uname test +adm",
		"uname test -test2",
		"users -w",
		"users -d",
		"users -r /active/adm/users",
	} {
		if err := cliExec(nil, cmd); err != nil {
			t.Errorf("%s: create new user: %v", cmd, err)
		}
	}
	cons, buf := testCons()
	if err := cliExec(cons, "uname -d"); err != nil {
		t.Errorf("dump user table: %v", err)
	}
	out := strings.TrimSpace(buf.String())

	want := `nuser 8 len = 140
adm:adm:adm:sys
glenda:glenda:glenda:
none:none::
noworld:noworld::
sys:sys::glenda
test:test::glenda,adm
test1:test2::
test4:test3:glenda:`

	if out != want {
		t.Errorf("bad user table: got %+s, want %+s", out, want)
	}

	fsys, err := getFsys("main")
	if err != nil {
		t.Fatalf("get fsys: %v", err)
	}
	if err := fsysClose(nil, fsys, []string{"close"}); err != nil {
		t.Fatalf("close fsys: %v", err)
	}
	fsys.put()
	if err := fsysUnconfig(nil, "main", []string{"unconfig"}); err != nil {
		t.Fatalf("unconfig fsys: %v", err)
	}
}
