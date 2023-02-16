package verification

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	datadriven.Walk(t, "testdata/datadriven", testDataDriven)
}

func testDataDriven(t *testing.T, path string) {
	ctx := context.Background()

	for _, pgurl := range []string{
		"postgres://localhost:5432/testdb",
		"postgres://root@127.0.0.1:26257/defaultdb?sslmode=disable",
	} {
		func() {
			conn, err := pgx.Connect(ctx, pgurl)
			require.NoError(t, err)
			defer func() { _ = conn.Close(ctx) }()

			_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS _ddtest")
			require.NoError(t, err)
			_, err = conn.Exec(ctx, "CREATE DATABASE _ddtest")
			require.NoError(t, err)
		}()
	}

	var conns []Conn
	for _, cfg := range []struct {
		name  string
		pgurl string
	}{
		{name: "truth", pgurl: "postgres://localhost:5432/_ddtest"},
		{name: "lie", pgurl: "postgres://root@127.0.0.1:26257/_ddtest?sslmode=disable"},
	} {
		conn, err := pgx.Connect(ctx, cfg.pgurl)
		require.NoError(t, err)
		conns = append(conns, Conn{ID: ConnID(cfg.name), Conn: conn})
	}
	defer func() {
		for _, conn := range conns {
			_ = conn.Conn.Close(ctx)
		}
	}()

	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		var sb strings.Builder
		switch td.Cmd {
		case "exec":
			var connIdxs []int
			for _, arg := range td.CmdArgs {
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
				tag, err := conns[connIdx].Conn.Exec(ctx, td.Input)
				if err != nil {
					sb.WriteString(fmt.Sprintf("[conn %d] error: %s\n", connIdx, err.Error()))
					continue
				}
				sb.WriteString(fmt.Sprintf("[conn %d] %s\n", connIdx, tag.String()))
			}
		case "verify":
			reporter := &LogReporter{
				Printf: func(f string, args ...any) {
					sb.WriteString(fmt.Sprintf(f, args...))
					sb.WriteRune('\n')
				},
			}
			err := Verify(ctx, conns, reporter)
			if err != nil {
				sb.WriteString(fmt.Sprintf("error: %s\n", err.Error()))
			}
		default:
			t.Fatalf("unknown command: %s", td.Cmd)
		}
		return sb.String()
	})
}
