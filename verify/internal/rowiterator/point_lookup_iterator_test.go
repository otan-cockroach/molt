package rowiterator

import (
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/stretchr/testify/require"
)

func TestPointLookupQuery(t *testing.T) {
	datadriven.Walk(t, "testdata/pointlookup", func(t *testing.T, path string) {
		var pt pointLookupIterator
		pt.rowBatchSize = 2
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "table":
				pt.table = tableFromCreateTable(t, d.Input)
				return ""
			case "pks":
				pt.pks = pt.pks[:0]
				for _, line := range strings.Split(d.Input, "\n") {
					pt.pks = append(pt.pks, parseDatums(t, line, ","))
				}
				return ""
			case "mysql":
				pt.conn = &dbconn.MySQLConn{}
				q, err := pt.genQuery()
				require.NoError(t, err)
				return q
			case "pg":
				pt.conn = &dbconn.PGConn{}
				q, err := pt.genQuery()
				require.NoError(t, err)
				return q
			}
			t.Errorf("unknown command %s", d.Cmd)
			return ""
		})
	})
}
