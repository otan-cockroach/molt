package dbverify

import (
	"testing"

	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/verify/inconsistency"
	"github.com/cockroachdb/molt/verify/verifybase"
	"github.com/stretchr/testify/require"
)

func TestTableCompare(t *testing.T) {
	table1 := verifybase.DBTable{
		OID: 1,
		TableName: verifybase.TableName{
			Schema: "1",
			Table:  "1",
		},
	}
	table2 := verifybase.DBTable{
		OID: 2,
		TableName: verifybase.TableName{
			Schema: "2",
			Table:  "2",
		},
	}
	table3 := verifybase.DBTable{
		OID: 3,
		TableName: verifybase.TableName{
			Schema: "3",
			Table:  "3",
		},
	}
	table3DifferentOID := verifybase.DBTable{
		OID: 33,
		TableName: verifybase.TableName{
			Schema: "3",
			Table:  "3",
		},
	}
	conn1 := dbconn.MakeFakeConn("1")
	conn2 := dbconn.MakeFakeConn("1")
	for _, tc := range []struct {
		desc     string
		its      [2]tableVerificationIterator
		expected Result
	}{
		{
			desc: "exactly the same",
			its: [2]tableVerificationIterator{
				{
					tables: connWithTables{
						Conn:          conn1,
						tableMetadata: []verifybase.DBTable{table1, table2, table3},
					},
				},
				{
					tables: connWithTables{
						Conn:          conn2,
						tableMetadata: []verifybase.DBTable{table1, table2, table3DifferentOID},
					},
				},
			},
			expected: Result{
				Verified: [][2]verifybase.DBTable{
					{table1, table1},
					{table2, table2},
					{table3, table3DifferentOID},
				},
			},
		},
		{
			desc: "everything missing",
			its: [2]tableVerificationIterator{
				{
					tables: connWithTables{
						Conn:          conn1,
						tableMetadata: []verifybase.DBTable{table1, table2, table3},
					},
				},
				{
					tables: connWithTables{
						Conn: conn2,
					},
				},
			},
			expected: Result{
				MissingTables: []inconsistency.MissingTable{
					{ConnID: conn2.ID(), DBTable: table1},
					{ConnID: conn2.ID(), DBTable: table2},
					{ConnID: conn2.ID(), DBTable: table3},
				},
			},
		},
		{
			desc: "everything extraneous",
			its: [2]tableVerificationIterator{
				{
					tables: connWithTables{
						Conn: conn1,
					},
				},
				{
					tables: connWithTables{
						Conn:          conn2,
						tableMetadata: []verifybase.DBTable{table1, table2, table3},
					},
				},
			},
			expected: Result{
				ExtraneousTables: []inconsistency.ExtraneousTable{
					{ConnID: conn2.ID(), DBTable: table1},
					{ConnID: conn2.ID(), DBTable: table2},
					{ConnID: conn2.ID(), DBTable: table3},
				},
			},
		},
		{
			desc: "only table 2 in common",
			its: [2]tableVerificationIterator{
				{
					tables: connWithTables{
						Conn:          conn1,
						tableMetadata: []verifybase.DBTable{table1, table2},
					},
				},
				{
					tables: connWithTables{
						Conn:          conn2,
						tableMetadata: []verifybase.DBTable{table2, table3},
					},
				},
			},
			expected: Result{
				Verified: [][2]verifybase.DBTable{
					{table2, table2},
				},
				MissingTables: []inconsistency.MissingTable{
					{ConnID: conn2.ID(), DBTable: table1},
				},
				ExtraneousTables: []inconsistency.ExtraneousTable{
					{ConnID: conn2.ID(), DBTable: table3},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, compare(tc.its))
		})
	}
}
