package verification

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/lib/pq/oid"
)

type TableMetadata struct {
	OID    oid.Oid
	Schema tree.Name
	Table  tree.Name
}

type connWithTables struct {
	dbconn.Conn
	tableMetadata []TableMetadata
}

type databaseTableVerificationResult struct {
	verified [2][]TableMetadata

	missingTables    []MissingTable
	extraneousTables []ExtraneousTable
}

type tableVerificationIterator struct {
	table   connWithTables
	currIdx int
}

func (c *tableVerificationIterator) done() bool {
	return c.currIdx >= len(c.table.tableMetadata)
}

func (c *tableVerificationIterator) next() {
	c.currIdx++
}

func (c *tableVerificationIterator) curr() TableMetadata {
	return c.table.tableMetadata[c.currIdx]
}

// verifyDatabaseTables verifies tables exist in all databases.
func verifyDatabaseTables(
	ctx context.Context, conns dbconn.OrderedConns,
) (databaseTableVerificationResult, error) {
	ret := databaseTableVerificationResult{}

	// Grab all tables and verify them.
	var in []connWithTables
	for _, conn := range conns {
		var tms []TableMetadata
		switch conn := conn.(type) {
		case *dbconn.MySQLConn:
			rows, err := conn.QueryContext(
				ctx,
				`SELECT table_name FROM information_schema.tables
WHERE table_schema = database() AND table_type = "BASE TABLE"
ORDER BY table_name`,
			)
			if err != nil {
				return ret, err
			}

			for rows.Next() {
				// Fake the public schema for now.
				tm := TableMetadata{
					Schema: "public",
				}
				if err := rows.Scan(&tm.Table); err != nil {
					return ret, errors.Wrap(err, "error decoding table metadata")
				}
				tms = append(tms, tm)
			}
			if rows.Err() != nil {
				return ret, errors.Wrap(err, "error collecting table metadata")
			}
		case *dbconn.PGConn:
			rows, err := conn.Query(
				ctx,
				`SELECT pg_class.oid, pg_class.relname, pg_namespace.nspname
FROM pg_class
JOIN pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
WHERE relkind = 'r' AND pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')
ORDER BY 3, 2`,
			)
			if err != nil {
				return ret, err
			}

			for rows.Next() {
				var tm TableMetadata
				if err := rows.Scan(&tm.OID, &tm.Table, &tm.Schema); err != nil {
					return ret, errors.Wrap(err, "error decoding table metadata")
				}
				tms = append(tms, tm)
			}
			if rows.Err() != nil {
				return ret, errors.Wrap(err, "error collecting table metadata")
			}
		default:
			return ret, errors.Newf("connection %T not supported", conn)
		}

		// Sort tables by schemas and names.
		sort.Slice(tms, func(i, j int) bool {
			return tms[i].Less(tms[j])
		})
		in = append(in, connWithTables{
			Conn:          conn,
			tableMetadata: tms,
		})
	}

	var iterators [2]tableVerificationIterator
	for i := range in {
		iterators[i] = tableVerificationIterator{
			table: in[i],
		}
	}

	// Iterate through all tables in source of truthIterator, moving iterators
	// across
	truthIterator := &iterators[0]
	nonTruthIterator := &iterators[1]
	for !truthIterator.done() {
		// If the iterator is done, that means we are missing tables
		// from the truth value. Mark nonTruthIterator as 1 to signify nonTruthIterator as a missing
		// table.
		compareVal := 1
		if !nonTruthIterator.done() {
			compareVal = nonTruthIterator.curr().Compare(truthIterator.curr())
		}
		switch compareVal {
		case -1:
			// Extraneous row compared to source of truthIterator.
			ret.extraneousTables = append(
				ret.extraneousTables,
				ExtraneousTable{ConnID: nonTruthIterator.table.ID(), TableMetadata: nonTruthIterator.curr()},
			)
			nonTruthIterator.next()
		case 0:
			for i := range iterators {
				ret.verified[i] = append(ret.verified[i], iterators[i].curr())
			}
			nonTruthIterator.next()
			truthIterator.next()
		case 1:
			// Missing a row from source of truth.
			ret.missingTables = append(
				ret.missingTables,
				MissingTable{ConnID: nonTruthIterator.table.ID(), TableMetadata: truthIterator.curr()},
			)
			truthIterator.next()
		}
	}

	for !nonTruthIterator.done() {
		ret.extraneousTables = append(
			ret.extraneousTables,
			ExtraneousTable{ConnID: nonTruthIterator.table.ID(), TableMetadata: nonTruthIterator.curr()},
		)
		nonTruthIterator.next()
	}
	return ret, nil
}
