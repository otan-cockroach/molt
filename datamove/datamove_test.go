package datamove

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/testutils"
	"github.com/cockroachdb/molt/verify/dbverify"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	for _, tc := range []struct {
		desc string
		path string
		src  string
		dest string
	}{
		{desc: "pg", path: "testdata/pg", src: testutils.PGConnStr(), dest: testutils.CRDBConnStr()},
		{desc: "mysql", path: "testdata/mysql", src: testutils.MySQLConnStr(), dest: testutils.CRDBConnStr()},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			datadriven.Walk(t, tc.path, func(t *testing.T, path string) {
				ctx := context.Background()
				var conns dbconn.OrderedConns
				var err error
				dbName := "datamove_datadriven_" + strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
				logger := zerolog.New(os.Stderr)

				conns[0], err = dbconn.TestOnlyCleanDatabase(ctx, "source", tc.src, dbName)
				require.NoError(t, err)
				conns[1], err = dbconn.TestOnlyCleanDatabase(ctx, "target", tc.dest, dbName)
				require.NoError(t, err)

				datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
					// Extract common arguments.
					args := d.CmdArgs[:0]
					var expectError bool
					for _, arg := range d.CmdArgs {
						switch arg.Key {
						case "expect-error":
							expectError = true
						default:
							args = append(args, arg)
						}
					}
					d.CmdArgs = args

					switch d.Cmd {
					case "exec":
						return testutils.ExecConnCommand(t, d, conns)
					case "query":
						return testutils.QueryConnCommand(t, d, conns)
					case "datamove":
						filter := dbverify.DefaultFilterConfig()
						truncate := true
						live := false

						for _, cmd := range d.CmdArgs {
							switch cmd.Key {
							case "live":
								live = true
							case "notruncate":
								truncate = false
							default:
								t.Errorf("unknown key %s", cmd.Key)
							}
						}
						dir, err := os.MkdirTemp("", "")
						require.NoError(t, err)
						src, err := datamovestore.NewLocalStore(logger, dir, "localhost:4000", "localhost:4000")
						require.NoError(t, err)

						err = DataMove(ctx, Config{Live: live, Truncate: truncate}, logger, conns, src, filter)
						if expectError {
							require.Error(t, err)
							return err.Error()
						}
						require.NoError(t, err)
						return ""
					default:
						t.Errorf("unknown command: %s", d.Cmd)
					}

					return ""
				})
			})
		})
	}
}
