package verification

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
	Schema string
	Table  string

	PrimaryKeyColumns []columnName
	PrimaryKeyValues  []any
}

type ExtraneousRow struct {
	ConnID ConnID
	Schema string
	Table  string

	PrimaryKeyColumns []columnName
	PrimaryKeyValues  []any
}

type MismatchingRow struct {
	ConnID ConnID
	Schema string
	Table  string

	PrimaryKeyColumns []columnName
	PrimaryKeyValues  []any

	MismatchingColumns []columnName
	TruthVals          []any
	TargetVals         []any
}

type StatusReport struct {
	Info string
}
