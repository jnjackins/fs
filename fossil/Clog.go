package main

import "fmt"

func consPrintf(format string, args ...interface{}) int {
	return consWrite([]byte(fmt.Sprintf(format, args...)))
}
