package verification

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func TestConvertRowValue(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgURLs()[0])
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()

	datadriven.Walk(t, "testdata/rowvalue", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {

			var sb strings.Builder
			switch d.Cmd {
			case "convert":
				rows, err := conn.Query(ctx, "SELECT "+d.Input)
				require.NoError(t, err)
				for rows.Next() {
					vals, err := rows.Values()
					require.NoError(t, err)

					for i, val := range vals {
						converted, err := convertRowValue(conn.TypeMap(), val, oid.Oid(rows.FieldDescriptions()[i].DataTypeOID))
						require.NoError(t, err)

						extra := ""
						if t := converted.ResolvedType().ArrayContents(); t != nil {
							extra += fmt.Sprintf(" (%s)", t.SQLString())
						}
						sb.WriteString(fmt.Sprintf("%T%s: %s", converted, extra, converted.String()))
					}
				}
				require.NoError(t, rows.Err())
				return sb.String()
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return sb.String()
		})
	})
}
