package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"
)

var (
	mempcnt  int    /* for 9fsys.c */
	foptname string = "/none/such"
)

func readCmdPart(file string, cmd []string) []string {
	fd, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	if _, err := fd.Seek(127*1024, 0); err != nil {
		log.Fatalf("seek %s 127kB: %v", file, err)
	}
	buf := make([]byte, 1024)
	n, err := fd.Read(buf)
	if n == 0 {
		log.Fatalf("short read of %s at 127kB", file)
	}
	if err != nil {
		log.Fatalf("read %s: %v", file, err)
	}
	if string(buf[:6+1+6+1]) != "fossil config\n" {
		log.Fatalf("bad config magic in %s", file)
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

func serve(argv []string) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-c cmd] [-f partition] [-m %%]\n", argv0)
		flags.PrintDefaults()
		os.Exit(1)
	}
	var (
		cflag = flags.String("c", "", "execute the console command `cmd`")
		fflag = flags.String("f", "", "read and execute console commands stored in the Fossil disk `file`")
		mflag = flags.Int("m", 30, "allocate `%` percent of the available free RAM for buffers")
	)
	err := flags.Parse(argv)
	if err != nil {
		flags.Usage()
	}

	var cmd []string
	if *cflag != "" {
		currfsysname = *cflag
		cmd = append(cmd, *cflag)
	}
	if *fflag != "" {
		foptname = *fflag
		currfsysname = foptname
		cmd = readCmdPart(*fflag, cmd)
	}
	mempcnt = *mflag
	if mempcnt <= 0 || mempcnt >= 100 {
		flags.Usage()
	}

	if flags.NArg() != 0 {
		flags.Usage()
	}

	consInit()
	cliInit()
	msgInit()
	conInit()
	cmdInit()
	fsysInit()
	exclInit()
	//fidInit()

	//srvInit()
	//lstnInit()
	usersInit()

	for i := 0; i < len(cmd); i++ {
		if err := cliExec(cmd[i]); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", cmd[i], err)
		}
	}

	if err := consTTY(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	consProc()
}
