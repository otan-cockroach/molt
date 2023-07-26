package verify

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/testutils"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type connArg struct {
	id      dbconn.ID
	connStr string
}

func TestDataDrivenPG(t *testing.T) {
	datadriven.Walk(
		t,
		"testdata/datadriven/pg",
		func(t *testing.T, path string) {
			testDataDriven(t, path, []connArg{
				{id: "pg", connStr: testutils.PGConnStr()},
				{id: "crdb", connStr: testutils.CRDBConnStr()},
			})
		},
	)
}

func TestDataDrivenMySQL(t *testing.T) {
	datadriven.Walk(
		t,
		"testdata/datadriven/mysql",
		func(t *testing.T, path string) {
			testDataDriven(t, path, []connArg{
				{id: "mysql", connStr: testutils.MySQLConnStr()},
				{id: "crdb", connStr: testutils.CRDBConnStr()},
			})
		},
	)
}

func testDataDriven(t *testing.T, path string, connArgs []connArg) {
	ctx := context.Background()

	var conns dbconn.OrderedConns
	for i, args := range connArgs {
		cleanConn, err := dbconn.TestOnlyCleanDatabase(ctx, args.id, args.connStr, "dd_test")
		require.NoError(t, err)
		conns[i] = cleanConn
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
				switch conn := conns[connIdx].(type) {
				case *dbconn.PGConn:
					tag, err := conn.Exec(ctx, d.Input)
					if err != nil {
						sb.WriteString(fmt.Sprintf("[%s] error: %s\n", conn.ID(), err.Error()))
						continue
					}
					sb.WriteString(fmt.Sprintf("[%s] %s\n", conn.ID(), tag.String()))
				case *dbconn.MySQLConn:
					tag, err := conn.ExecContext(ctx, d.Input)
					if err != nil {
						sb.WriteString(fmt.Sprintf("[%s] error: %s\n", conn.ID(), err.Error()))
						continue
					}
					r, err := tag.RowsAffected()
					if err != nil {
						sb.WriteString(fmt.Sprintf("[%s] error getting rows affected: %s\n", conn.ID(), err.Error()))
						continue
					}
					sb.WriteString(fmt.Sprintf("[%s] %d rows affected\n", conn.ID(), r))
				default:
					t.Fatalf("unhandled Conn type: %T", conn)
				}
			}

			// Deallocate caches - otherwise the plans may stick around.
			for _, conn := range conns {
				switch conn := conn.(type) {
				case *dbconn.PGConn:
					require.NoError(t, conn.DeallocateAll(ctx))
				}
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
				Logger: zerolog.New(&sb),
			}
			// Use 1 concurrency / splitting to ensure deterministic results.
			err := Verify(
				ctx,
				conns,
				zerolog.Nop(),
				reporter,
				WithConcurrency(1),
				WithRowBatchSize(2),
				WithTableSplits(numSplits),
			)
			if err != nil {
				sb.WriteString(fmt.Sprintf("error: %s\n", err.Error()))
			}
		default:
			t.Fatalf("unknown command: %s", d.Cmd)
		}
		return sb.String()
	})
}
