package verifybase

import (
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

type TableName struct {
	Schema tree.Name
	Table  tree.Name
}

// DBTable represents a basic table object with OID from the relevant table.
type DBTable struct {
	TableName
	OID oid.Oid
}

func (tm DBTable) Compare(o DBTable) int {
	if c := strings.Compare(strings.ToLower(string(tm.Schema)), strings.ToLower(string(o.Schema))); c != 0 {
		return c
	}
	return strings.Compare(strings.ToLower(string(tm.Table)), strings.ToLower(string(o.Table)))
}

func (tm DBTable) Less(o DBTable) bool {
	return tm.Compare(o) < 0
}

// VerifiableTable represents a table which can be verified.
type VerifiableTable struct {
	TableName
	PrimaryKeyColumns []tree.Name
	Columns           []tree.Name
	ColumnOIDs        [2][]oid.Oid
}
