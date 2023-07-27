package inconsistency

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/verify/verifybase"
)

type ReportableObject interface{}

type MissingRow struct {
	verifybase.TableName

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums

	Columns []tree.Name
	Values  tree.Datums
}

type ExtraneousRow struct {
	verifybase.TableName

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums
}

type MismatchingRow struct {
	verifybase.TableName

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums

	MismatchingColumns []tree.Name
	TruthVals          tree.Datums
	TargetVals         tree.Datums
}
