package verification

import (
	"context"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type TableMetadata struct {
	OID    oid.Oid
	Schema string
	Table  string
}

type connWithTables struct {
	Conn
	tableMetadata []TableMetadata
}

type databaseTableVerificationResult struct {
	verified map[ConnID][]TableMetadata

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
	ctx context.Context, conns []Conn,
) (databaseTableVerificationResult, error) {
	ret := databaseTableVerificationResult{
		verified: make(map[ConnID][]TableMetadata),
	}

	// Grab all tables and verify them.
	var in []connWithTables
	for _, conn := range conns {
		rows, err := conn.Conn.Query(
			ctx,
			`SELECT pg_class.oid, pg_class.relname, pg_namespace.nspname
FROM pg_class
JOIN pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
WHERE relkind = 'r' AND pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', 'pg_extension')`,
		)
		if err != nil {
			return ret, err
		}

		var tms []TableMetadata
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

		// Sort tables by schemas and names.
		sort.Slice(tms, func(i, j int) bool {
			return tms[i].Less(tms[j])
		})
		in = append(in, connWithTables{
			Conn:          conn,
			tableMetadata: tms,
		})
	}

	iterators := make([]tableVerificationIterator, len(in))
	for i := range in {
		iterators[i] = tableVerificationIterator{
			table: in[i],
		}
	}

	// Iterate through all tables in source of truthIterator, moving iterators
	// across
	truthIterator := &iterators[0]
	for !truthIterator.done() {
		truthNext := true
		commonOnAll := true

		var inCommon []int
		for i := 1; i < len(iterators); i++ {
			it := &iterators[i]

			// If the iterator is done, that means we are missing tables
			// from the truth value. Mark it as 1 to signify it as a missing
			// table.
			compareVal := 1
			if !it.done() {
				compareVal = it.curr().Compare(truthIterator.curr())
			}
			switch compareVal {
			case -1:
				// Extraneous row compared to source of truthIterator.
				ret.extraneousTables = append(
					ret.extraneousTables,
					ExtraneousTable{ConnID: it.table.ID, TableMetadata: it.curr()},
				)
				// Move the curr table over.
				commonOnAll = false
				it.next()
				truthNext = false
			case 0:
				// Found on this it.
				inCommon = append(inCommon, i)
			case 1:
				// Missing a row from source of truthIterator.
				ret.missingTables = append(
					ret.missingTables,
					MissingTable{ConnID: it.table.ID, TableMetadata: truthIterator.curr()},
				)
				commonOnAll = false
			}
		}

		// If the state is common, add the table metadata attributed to the current state.
		if commonOnAll {
			for i, it := range iterators {
				ret.verified[conns[i].ID] = append(
					ret.verified[conns[i].ID],
					it.curr(),
				)
			}
		}

		// Continue if available.
		if truthNext {
			truthIterator.next()
			// Also advance all connections which are in common.
			for _, idx := range inCommon {
				iterators[idx].next()
			}
		}
	}

	// There may still be extraneous tables from the remaining iterators.
	for i := 1; i < len(iterators); i++ {
		it := &iterators[i]
		for !it.done() {
			ret.extraneousTables = append(
				ret.extraneousTables,
				ExtraneousTable{ConnID: it.table.ID, TableMetadata: it.curr()},
			)
			it.next()
		}
	}
	return ret, nil
}
