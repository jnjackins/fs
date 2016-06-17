package main

import (
	"io/ioutil"
	"log"
	"os"
)

func testFormatFossil() (string, error) {
	tmpfile, err := ioutil.TempFile("", "fossil.part")
	if err != nil {
		log.Fatal(err)
	}
	path := tmpfile.Name()

	// 10k blocks
	buf := make([]byte, 8*1024)
	for i := 0; i < 10000; i++ {
		if _, err := tmpfile.Write(buf); err != nil {
			tmpfile.Close()
			os.Remove(path)
			return "", err
		}
	}

	tmpfile.Close()
	format([]string{"-b", "4K", "-y", path})

	return path, nil
}
