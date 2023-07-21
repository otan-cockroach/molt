package rowiterator

import (
	"context"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/dbconn"
)

type scanIterator struct {
	conn         dbconn.Conn
	table        Table
	rowBatchSize int

	waitCh        chan itResult
	cache         []tree.Datums
	pkCursor      tree.Datums
	currCacheSize int
	err           error
	scanQuery     scanQuery
}

type itResult struct {
	r   []tree.Datums
	err error
}

// NewScanIterator returns a row iterator which scans the given table.
func NewScanIterator(
	ctx context.Context, conn dbconn.Conn, table Table, rowBatchSize int,
) (Iterator, error) {
	// Initialize the type map on the connection.
	for _, typOID := range table.ColumnOIDs {
		if _, err := dbconn.GetDataType(ctx, conn, typOID); err != nil {
			return nil, errors.Wrapf(err, "Error initializing type oid %d", typOID)
		}
	}
	it := &scanIterator{
		conn:          conn,
		table:         table,
		rowBatchSize:  rowBatchSize,
		currCacheSize: rowBatchSize,
		waitCh:        make(chan itResult, 1),
	}
	switch conn := conn.(type) {
	case *dbconn.PGConn:
		it.scanQuery = newPGScanQuery(table, rowBatchSize)
	case *dbconn.MySQLConn:
		it.scanQuery = newMySQLScanQuery(table, rowBatchSize)
	default:
		return nil, errors.Newf("unsupported conn type %T", conn)
	}
	it.nextPage(ctx)
	return it, nil
}

func (it *scanIterator) Conn() dbconn.Conn {
	return it.conn
}

func (it *scanIterator) HasNext(ctx context.Context) bool {
	for {
		if it.err != nil {
			return false
		}

		if len(it.cache) > 0 {
			return true
		}

		// If the last cache size was less than the row size, we're done
		// reading all the results.
		if it.currCacheSize < it.rowBatchSize {
			return false
		}

		// Wait for more results.
		res := <-it.waitCh
		if res.err != nil {
			it.err = errors.Wrap(res.err, "error getting result")
			return false
		}
		it.cache = res.r
		it.currCacheSize = len(it.cache)

		// Queue the next page immediately.
		if it.currCacheSize == it.rowBatchSize {
			it.nextPage(ctx)
		}
	}
}

// nextPage fetches rows asynchronously.
func (it *scanIterator) nextPage(ctx context.Context) {
	go func() {
		datums, err := func() ([]tree.Datums, error) {
			var currRows rows
			q, err := it.scanQuery.generate(it.pkCursor)
			if err != nil {
				return nil, err
			}
			switch conn := it.conn.(type) {
			case *dbconn.PGConn:
				newRows, err := conn.Query(ctx, q)
				if err != nil {
					return nil, errors.Wrapf(err, "error getting rows for table %s.%s from %s", it.table.Schema, it.table.Table, it.conn.ID)
				}
				currRows = &pgRows{
					Rows:    newRows,
					typMap:  it.conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			case *dbconn.MySQLConn:
				newRows, err := conn.QueryContext(ctx, q)
				if err != nil {
					return nil, errors.Wrapf(err, "error getting rows for table %s in %s", it.table.Table, it.conn.ID())
				}
				currRows = &mysqlRows{
					Rows:    newRows,
					typMap:  it.conn.TypeMap(),
					typOIDs: it.table.ColumnOIDs,
				}
			default:
				return nil, errors.AssertionFailedf("unhandled conn type: %T", conn)
			}
			defer func() { currRows.Close() }()

			datums := make([]tree.Datums, 0, it.rowBatchSize)
			for currRows.Next() {
				d, err := currRows.Datums()
				if err != nil {
					return nil, errors.Wrapf(err, "error getting datums")
				}
				it.pkCursor = d[:len(it.table.PrimaryKeyColumns)]
				datums = append(datums, d)
			}
			return datums, currRows.Err()
		}()
		it.waitCh <- itResult{r: datums, err: err}
	}()
}

func (it *scanIterator) Peek(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		return it.cache[0]
	}
	return nil
}

func (it *scanIterator) Next(ctx context.Context) tree.Datums {
	if it.HasNext(ctx) {
		ret := it.cache[0]
		it.cache = it.cache[1:]
		return ret
	}
	return nil
}

func (it *scanIterator) Error() error {
	return it.err
}
