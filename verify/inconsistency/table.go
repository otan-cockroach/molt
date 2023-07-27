package inconsistency

import "github.com/cockroachdb/molt/verify/verifybase"

// MismatchingTableDefinition represents a missing table definition.
type MismatchingTableDefinition struct {
	verifybase.DBTable
	Info string
}
