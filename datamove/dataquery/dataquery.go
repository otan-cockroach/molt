package dataquery

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/rowiterator"
)

func NewPGCopyTo(table dbtable.VerifiedTable) string {
	copyFrom := &tree.CopyTo{
		Statement: rowiterator.NewPGBaseSelectClause(rowiterator.Table{
			Name:              table.Name,
			ColumnNames:       table.Columns,
			PrimaryKeyColumns: table.PrimaryKeyColumns,
		}),
		Options: tree.CopyOptions{
			CopyFormat: tree.CopyFormatCSV,
			HasFormat:  true,
		},
	}
	f := tree.NewFmtCtx(tree.FmtParsableNumerics)
	f.FormatNode(copyFrom)
	return f.CloseAndGetString()
}

func ImportInto(table dbtable.VerifiedTable, locs []string) string {
	importInto := &tree.Import{
		Into: true,
		// TODO: schema name
		Table:      tree.NewUnqualifiedTableName(table.Table),
		FileFormat: "CSV",
		IntoCols:   table.Columns,
	}
	for _, loc := range locs {
		importInto.Files = append(
			importInto.Files,
			tree.NewStrVal(loc),
		)
	}
	f := tree.NewFmtCtx(tree.FmtParsableNumerics)
	f.FormatNode(importInto)
	return f.CloseAndGetString()
}

func CopyFrom(table dbtable.VerifiedTable) string {
	importInto := &tree.CopyFrom{
		// TODO: schema name
		Table:   tree.MakeUnqualifiedTableName(table.Table),
		Columns: table.Columns,
		Options: tree.CopyOptions{
			CopyFormat: tree.CopyFormatCSV,
			HasFormat:  true,
		},
	}
	f := tree.NewFmtCtx(tree.FmtParsableNumerics)
	f.FormatNode(importInto)
	return f.CloseAndGetString()
}
