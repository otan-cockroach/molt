package inconsistency

import "github.com/cockroachdb/molt/verify/verifybase"

// MissingTable represents a table that is missing from a database.
type MissingTable struct {
	verifybase.DBTable
}

// ExtraneousTable represents a table that is extraneous to a database.
type ExtraneousTable struct {
	verifybase.DBTable
}
