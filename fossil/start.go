package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"unicode"
)

var (
	mempcnt  int    /* for 9fsys.c */
	foptname string = "/none/such"
)

func start(argv []string) {
	flags := flag.NewFlagSet("serve", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-t] [-c cmd] [-f partition] [-m %%]\n", argv0)
		flags.PrintDefaults()
		os.Exit(1)
	}
	var (
		cflag = flags.String("c", "", "Execute the console command `cmd`.")
		fflag = flags.String("f", "", "Read and execute console commands stored in the Fossil disk `file`.")
		mflag = flags.Int("m", 30, "Allocate `%` percent of the available free RAM for buffers.")
		tflag = flags.Bool("t", true, "Connect to the console on startup.")
	)
	flags.Parse(argv)

	var cmd []string
	if *cflag != "" {
		cmd = append(cmd, *cflag)
	}
	if *fflag != "" {
		foptname = *fflag
		cmd = readCmdPart(*fflag, cmd)
	}
	mempcnt = *mflag
	if mempcnt <= 0 || mempcnt >= 100 {
		flags.Usage()
	}

	if flags.NArg() != 0 {
		flags.Usage()
	}

	var cons *Cons
	if *tflag {
		tty, err := newTTY()
		if err != nil {
			fatalf("error opening tty: %v", err)
		}
		cons = tty
	}

	cliInit()
	msgInit()
	conInit()
	cmdInit()
	fsysInit()
	exclInit()
	fidInit()

	srvInit()
	lstnInit()
	usersInit()

	for i := 0; i < len(cmd); i++ {
		cons.printf("%s\n", cmd[i])
		if err := cliExec(cons, cmd[i]); err != nil {
			cons.printf("%v\n", err)
		}
	}

	runtime.Goexit()
}

func readCmdPart(file string, cmd []string) []string {
	fd, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	if _, err := fd.Seek(127*1024, 0); err != nil {
		fatalf("seek %s 127kB: %v", file, err)
	}
	buf := make([]byte, 1024)
	n, err := fd.Read(buf)
	if n == 0 {
		fatalf("short read of %s at 127kB", file)
	}
	if err != nil {
		fatalf("read %s: %v", file, err)
	}
	if string(buf[:6+1+6+1]) != "fossil config\n" {
		fatalf("bad config magic in %s", file)
	}

	f := strings.FieldsFunc(string(buf[6+1+6+1:]), func(c rune) bool { return c == '\n' })
	for i := 0; i < len(f); i++ {
		if f[i][0] == '#' {
			continue
		}

		// expand argument '*' to mean current file
		if j := strings.IndexByte(f[i], '*'); j >= 0 {
			if (j == 0 || isspace(f[i][j-1])) && (j == len(f[1])-1 || isspace(f[i][j+1])) {
				f[i] = f[i][:j] + file + f[i][j+1:]
			}
		}

		cmd = append(cmd, f[i])
	}

	return cmd
}

func isspace(c byte) bool {
	return unicode.IsSpace(rune(c))
}
