package inconsistency

import "github.com/cockroachdb/molt/dbtable"

// MismatchingTableDefinition represents a missing table definition.
type MismatchingTableDefinition struct {
	dbtable.DBTable
	Info string
}
