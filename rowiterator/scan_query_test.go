package rowiterator

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestScanQuery(t *testing.T) {
	datadriven.Walk(t, "testdata/scanquery", func(t *testing.T, path string) {
		var table Table
		var sq scanQuery
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "table":
				p, err := parser.ParseOne(d.Input)
				require.NoError(t, err)

				createTableStmt := p.AST.(*tree.CreateTable)
				table = Table{
					Schema: createTableStmt.Table.SchemaName,
					Table:  createTableStmt.Table.ObjectName,
				}
				for _, def := range createTableStmt.Defs {
					switch def := def.(type) {
					case *tree.ColumnTableDef:
						table.ColumnNames = append(table.ColumnNames, def.Name)
						table.ColumnOIDs = append(table.ColumnOIDs, def.Type.(*types.T).Oid())
					case *tree.UniqueConstraintTableDef:
						if def.PrimaryKey {
							for _, col := range def.Columns {
								table.PrimaryKeyColumns = append(table.PrimaryKeyColumns, col.Column)
							}
						}
					}
				}
				require.True(t, len(table.PrimaryKeyColumns) > 0, "primary key constraint must be explicitly defined")
				return ""
			case "start_pk":
				table.StartPKVals = parseDatums(t, d.Input)
				return ""
			case "end_pk":
				table.EndPKVals = parseDatums(t, d.Input)
				return ""
			case "mysql":
				sq = newMySQLScanQuery(table, 10000)
				return ""
			case "pg":
				sq = newPGScanQuery(table, 10000)
				return ""
			case "generate":
				require.NotNil(t, sq.base)
				s, err := sq.generate(parseDatums(t, d.Input))
				require.NoError(t, err)
				return s
			}
			t.Errorf("unknown command %s", d.Cmd)
			return ""
		})
	})
}

func parseDatums(t *testing.T, s string) tree.Datums {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var ret tree.Datums
	for _, v := range strings.Split(s, "\n") {
		ret = append(ret, tree.NewDString(v))
	}
	return ret
}
