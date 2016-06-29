package venti

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestMain(m *testing.M) {
	if err := exec.Command("../test/venti.sh").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error starting venti server for testing: %v\n", err)
		exec.Command("../test/clean.sh").Run()
		os.Exit(1)
	}

	defer os.Exit(m.Run())

	if err := exec.Command("../test/clean.sh").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error stopping venti server: %v\n", err)
		os.Exit(1)
	}
}
