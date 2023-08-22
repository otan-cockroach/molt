package cdcsink

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

func FindBinary(start string) (string, error) {
	p := filepath.Join(start, "cdc-sink")
	if _, err := os.Stat(p); err == nil {
		return p, nil
	}
	parent := filepath.Dir(start)
	if parent != start {
		return FindBinary(parent)
	}
	return "", errors.Newf("cdc-sink binary not found")
}
