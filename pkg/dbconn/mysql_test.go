package dbconn

import (
	"net/url"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

func TestMySQLURLToDSN(t *testing.T) {
	datadriven.Walk(t, "testdata/dsn", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "mysql":
				u, err := url.Parse(d.Input)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "error converting %s to url", d.Input))
				}
				return MySQLURLToDSN(u).FormatDSN()
			}
			t.Fatalf("unknown command %s", d.Cmd)
			return ""
		})
	})
}
