package verification

import (
	"context"
	"log"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/ctxgroup"
	"github.com/jackc/pgx/v5"
)

type ConnID string

type Conn struct {
	Conn *pgx.Conn
	ID   ConnID
}

func (tm TableMetadata) Compare(o TableMetadata) int {
	if c := strings.Compare(tm.Schema, o.Schema); c != 0 {
		return c
	}
	return strings.Compare(tm.Table, o.Table)
}

func (tm TableMetadata) Less(o TableMetadata) bool {
	return tm.Compare(o) < 0
}

const DefaultConcurrency = 8
const DefaultRowBatchSize = 1000

type VerifyOpt func(*verifyOpts)

type verifyOpts struct {
	concurrency  int
	rowBatchSize int
}

func WithConcurrency(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.concurrency = c
	}
}

func WithRowBatchSize(c int) VerifyOpt {
	return func(o *verifyOpts) {
		o.rowBatchSize = c
	}
}

// Verify verifies the given connections have matching tables and contents.
func Verify(ctx context.Context, conns []Conn, reporter Reporter, inOpts ...VerifyOpt) error {
	opts := verifyOpts{
		concurrency:  DefaultConcurrency,
		rowBatchSize: DefaultRowBatchSize,
	}
	for _, applyOpt := range inOpts {
		applyOpt(&opts)
	}

	ret, err := verifyDatabaseTables(ctx, conns)
	if err != nil {
		return errors.Wrap(err, "error comparing database tables")
	}

	for _, missingTable := range ret.missingTables {
		reporter.Report(missingTable)
	}
	for _, extraneousTable := range ret.extraneousTables {
		reporter.Report(extraneousTable)
	}

	// Grab columns for each table on both sides.
	tbls, err := verifyCommonTables(ctx, conns, ret.verified)
	if err != nil {
		return err
	}

	// Report mismatching table definitions.
	for _, tbl := range tbls {
		for _, d := range tbl.MismatchingTableDefinitions {
			reporter.Report(d)
		}
	}

	// Compare rows up to the concurrency specified.
	g := ctxgroup.WithContext(ctx)
	workQueue := make(chan verifyTableResult)
	for it := 0; it < opts.concurrency; it++ {
		g.GoCtx(func(ctx context.Context) error {
			for {
				work, ok := <-workQueue
				if !ok {
					return nil
				}
				if err := verifyDataWorker(ctx, conns, reporter, opts.rowBatchSize, work); err != nil {
					log.Printf("[ERROR] error comparing rows on %s.%s: %v", work.Schema, work.Table, err)
				}
			}
		})
	}
	for _, tbl := range tbls {
		workQueue <- tbl
	}
	close(workQueue)
	return g.Wait()
}

func verifyDataWorker(
	ctx context.Context, conns []Conn, reporter Reporter, rowBatchSize int, tbl verifyTableResult,
) error {
	// Copy connections over naming wise, but initialize a new pgx connection
	// for each table.
	workerConns := make([]Conn, len(conns))
	copy(workerConns, conns)
	for i := range workerConns {
		i := i
		var err error
		workerConns[i].Conn, err = pgx.ConnectConfig(ctx, conns[i].Conn.Config())
		if err != nil {
			return errors.Wrap(err, "error establishing connection to compare")
		}
		defer func() {
			_ = workerConns[i].Conn.Close(ctx)
		}()
	}
	return compareRows(ctx, workerConns, tbl, rowBatchSize, reporter)
}
