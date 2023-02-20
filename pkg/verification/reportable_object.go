package verification

import "github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

type ReportableObject interface{}

type MissingTable struct {
	ConnID ConnID
	TableMetadata
}

type ExtraneousTable struct {
	ConnID ConnID
	TableMetadata
}

type MismatchingTableDefinition struct {
	ConnID ConnID
	TableMetadata
	Info string
}

type MissingRow struct {
	ConnID ConnID
	Schema tree.Name
	Table  tree.Name

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums
}

type ExtraneousRow struct {
	ConnID ConnID
	Schema tree.Name
	Table  tree.Name

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums
}

type MismatchingRow struct {
	ConnID ConnID
	Schema tree.Name
	Table  tree.Name

	PrimaryKeyColumns []tree.Name
	PrimaryKeyValues  tree.Datums

	MismatchingColumns []tree.Name
	TruthVals          tree.Datums
	TargetVals         tree.Datums
}

type StatusReport struct {
	Info string
}
