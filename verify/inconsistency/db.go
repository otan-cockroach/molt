package inconsistency

import "github.com/cockroachdb/molt/dbtable"

// MissingTable represents a table that is missing from a database.
type MissingTable struct {
	dbtable.DBTable
}

// ExtraneousTable represents a table that is extraneous to a database.
type ExtraneousTable struct {
	dbtable.DBTable
}
