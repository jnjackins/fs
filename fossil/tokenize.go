package main

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func getToken(s string) (token, remainder string) {
	var quoting bool

	for len(s) > 0 {
		r, n := utf8.DecodeRuneInString(s)

		if !quoting && unicode.IsSpace(r) {
			break
		}

		if r != '\'' {
			token += string(r)
			s = s[n:]
			continue
		}

		// r is a quote
		if !quoting {
			quoting = true
			s = s[1:]
			continue
		}

		// quoting and we're on a quote
		if len(s) == 1 || s[1] != '\'' {
			//end of quoted section; absorb closing quote
			s = s[1:]
			quoting = false
			continue
		}

		// doubled quote; output one quote
		s = s[2:]
		token += "'"
	}

	return token, s
}

func tokenize(s string) []string {
	var tokens []string

	for len(s) > 0 {
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		if len(s) == 0 {
			break
		}
		token, remainder := getToken(s)
		tokens = append(tokens, token)

		s = remainder
	}

	return tokens
}
