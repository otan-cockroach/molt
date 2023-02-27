package verification

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/molt/pkg/dbconn"
	"github.com/cockroachdb/molt/pkg/testutils"
	"github.com/stretchr/testify/require"
)

type connArg struct {
	id      dbconn.ID
	connStr string
}

func TestDataDriven(t *testing.T) {
	datadriven.Walk(t, "testdata/datadriven", testDataDriven)
}

func testDataDriven(t *testing.T, path string) {
	ctx := context.Background()

	pgConnArgs := []connArg{
		{id: "truth", connStr: testutils.PGConnStr()},
		{id: "lie", connStr: testutils.CRDBConnStr()},
	}

	var conns []dbconn.Conn
	for _, pgArgs := range pgConnArgs {
		cleanConn, err := dbconn.TestOnlyCleanDatabase(ctx, pgArgs.id, pgArgs.connStr, "dd_test")
		require.NoError(t, err)
		conns = append(conns, cleanConn)
	}

	defer func() {
		for _, conn := range conns {
			_ = conn.Close(ctx)
		}
	}()

	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		var sb strings.Builder
		switch d.Cmd {
		case "exec":
			var connIdxs []int
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "source_of_truth":
					connIdxs = append(connIdxs, 0)
				case "non_source_of_truth":
					connIdxs = append(connIdxs, 1)
				case "all":
					for connIdx := range conns {
						connIdxs = append(connIdxs, connIdx)
					}
				}
			}
			require.NotEmpty(t, connIdxs, "destination sql must be defined")
			for _, connIdx := range connIdxs {
				tag, err := conns[connIdx].(*dbconn.PGConn).Exec(ctx, d.Input)
				if err != nil {
					sb.WriteString(fmt.Sprintf("[%s] error: %s\n", conns[connIdx].ID(), err.Error()))
					continue
				}
				sb.WriteString(fmt.Sprintf("[%s] %s\n", conns[connIdx].ID(), tag.String()))
			}

			// Deallocate caches - otherwise the plans may stick around.
			for _, conn := range conns {
				require.NoError(t, conn.(*dbconn.PGConn).DeallocateAll(ctx))
			}
		case "verify":
			numSplits := 1
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "splits":
					var err error
					numSplits, err = strconv.Atoi(arg.Vals[0])
					require.NoError(t, err)
				}
			}
			reporter := &LogReporter{
				Printf: func(f string, args ...any) {
					sb.WriteString(fmt.Sprintf(f, args...))
					sb.WriteRune('\n')
				},
			}
			// Use 1 concurrency / splitting to ensure deterministic results.
			err := Verify(ctx, conns, reporter, WithConcurrency(1), WithRowBatchSize(2), WithTableSplits(numSplits))
			if err != nil {
				sb.WriteString(fmt.Sprintf("error: %s\n", err.Error()))
			}
		default:
			t.Fatalf("unknown command: %s", d.Cmd)
		}
		return sb.String()
	})
}
