package inconsistency

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/dbconn"
)

type ReportableObject interface{}

type MissingRow struct {
	ConnID dbconn.ID
	Schema tree.Name
	Table  tree.Name

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums
	Columns           []tree.Name
	Values            tree.Datums
}

type ExtraneousRow struct {
	ConnID dbconn.ID
	Schema tree.Name
	Table  tree.Name

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums
}

type MismatchingRow struct {
	ConnID dbconn.ID
	Schema tree.Name
	Table  tree.Name

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums

	MismatchingColumns []tree.Name
	TruthVals          tree.Datums
	TargetVals         tree.Datums
}
