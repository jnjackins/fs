package main

import "fmt"

/*
 * To do: This will become 'print'.
 */
func consPrintf(format string, args ...interface{}) int {
	return consWrite([]byte(fmt.Sprintf(format, args...)))
}
