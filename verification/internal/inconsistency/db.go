package inconsistency

import (
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/verification/verifybase"
)

// MissingTable represents a table that is missing from a database.
type MissingTable struct {
	ConnID dbconn.ID
	verifybase.DBTable
}

// ExtraneousTable represents a table that is extraneous to a database.
type ExtraneousTable struct {
	ConnID dbconn.ID
	verifybase.DBTable
}
