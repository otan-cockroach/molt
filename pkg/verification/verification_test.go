package verification

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func pgURLs() []string {
	pgInstanceURL := "postgres://postgres:postgres@localhost:5432/testdb"
	if override, ok := os.LookupEnv("POSTGRES_URL"); ok {
		pgInstanceURL = override
	}
	crdbInstanceURL := "postgres://root@127.0.0.1:26257/defaultdb?sslmode=disable"
	if override, ok := os.LookupEnv("COCKROACH_URL"); ok {
		crdbInstanceURL = override
	}
	return []string{pgInstanceURL, crdbInstanceURL}
}

func TestDataDriven(t *testing.T) {
	datadriven.Walk(t, "testdata/datadriven", testDataDriven)
}

func testDataDriven(t *testing.T, path string) {
	ctx := context.Background()

	const dbName = "_ddtest"

	var cfgs []*pgx.ConnConfig
	for _, pgurl := range pgURLs() {
		func() {
			conn, err := pgx.Connect(ctx, pgurl)
			require.NoError(t, err)
			defer func() { _ = conn.Close(ctx) }()

			_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
			require.NoError(t, err)
			_, err = conn.Exec(ctx, "CREATE DATABASE "+dbName)
			require.NoError(t, err)

			cfgCopy := conn.Config().Copy()
			cfgCopy.Database = dbName
			cfgs = append(cfgs, cfgCopy)
		}()
	}

	var conns []Conn
	for i, cfg := range []struct {
		name string
	}{
		{name: "truth"},
		{name: "lie"},
	} {
		conn, err := pgx.ConnectConfig(ctx, cfgs[i])
		require.NoError(t, err)
		conns = append(conns, Conn{ID: ConnID(cfg.name), Conn: conn})
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.Conn.Close(ctx)
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
				tag, err := conns[connIdx].Conn.Exec(ctx, d.Input)
				if err != nil {
					sb.WriteString(fmt.Sprintf("[conn %d] error: %s\n", connIdx, err.Error()))
					continue
				}
				sb.WriteString(fmt.Sprintf("[conn %d] %s\n", connIdx, tag.String()))
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
