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

type MismatchingRow struct {
	TableName  string
	TargetName string

	PrimaryKey       []string
	PrimaryKeyValues []any

	Columns   []string
	TruthVals []any
	RowVals   []any
}
