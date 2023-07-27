//go:build tools
// +build tools

package main

import (
	_ "github.com/cockroachdb/crlfmt"
	_ "github.com/jstemmer/go-junit-report"
)
