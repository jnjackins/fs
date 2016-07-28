package main

import (
	"strings"
	"testing"
)

func TestCmd(t *testing.T) {
	if err := testAllocFsys(); err != nil {
		t.Fatalf("testAllocFsys: %v", err)
	}

	t.Run("cmdPrintConfig", func(t *testing.T) { testCmdPrintConfig(t, nil) })
	t.Run("cmdMsg", func(t *testing.T) { testCmdMsg(t, nil) })
	t.Run("cmdWho", func(t *testing.T) { testCmdWho(t, nil) })
	t.Run("cmdCon", func(t *testing.T) { testCmdCon(t, nil) })

	if err := testCleanupFsys(); err != nil {
		t.Fatalf("testCleanupFsys: %v", err)
	}
}

func testCmdPrintConfig(t *testing.T, cons *Cons) {
	if err := cmdPrintConfig(cons, strings.Fields("printconfig")); err != nil {
		t.Fatal(err)
	}
}

func testCmdMsg(t *testing.T, cons *Cons) {
	if err := cmdMsg(cons, strings.Fields("msg")); err != nil {
		t.Fatal(err)
	}
	if err := cmdMsg(cons, strings.Fields("msg -m 1 -p 1")); err != nil {
		t.Fatal(err)
	}
	if err := cmdMsg(cons, strings.Fields("msg -m=100 -p=100")); err != nil {
		t.Fatal(err)
	}
}

func testCmdWho(t *testing.T, cons *Cons) {
	// first run with no open fids
	if err := cmdWho(cons, strings.Fields("who")); err != nil {
		t.Fatalf("cmdWho: %v", err)
	}

	// create some fids for who to sort through
	if err := cmd9p(cons, strings.Fields("9p Tversion 8192 9P2000")); err != nil {
		t.Fatalf("cmd9p: %v\n", err)
	}
	if err := cmd9p(cons, strings.Fields("9p Tattach 0 ~1 nobody testfs/active")); err != nil {
		t.Fatalf("cmd9p: %v\n", err)
	}
	if err := cmd9p(cons, strings.Fields("9p Tattach 1 ~1 nobody testfs/snapshot")); err != nil {
		t.Fatalf("cmd9p: %v\n", err)
	}

	// TODO: check output
	if err := cmdWho(cons, strings.Fields("who")); err != nil {
		t.Fatalf("cmdWho: %v", err)
	}

	// clean up the fids we created
	if err := cmd9p(cons, strings.Fields("9p Tclunk 1")); err != nil {
		t.Fatalf("cmd9p: %v\n", err)
	}
	if err := cmd9p(cons, strings.Fields("9p Tclunk 0")); err != nil {
		t.Fatalf("cmd9p: %v\n", err)
	}
}

func testCmdCon(t *testing.T, cons *Cons) {
	if err := cmdCon(cons, strings.Fields("con")); err != nil {
		t.Fatal(err)
	}
	if err := cmdCon(cons, strings.Fields("con -m 1")); err != nil {
		t.Fatal(err)
	}
	if err := cmdCon(cons, strings.Fields("msg -m=100")); err != nil {
		t.Fatal(err)
	}
}
