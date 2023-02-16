package verification

import (
	"context"
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

type OID uint32

func (tm TableMetadata) Compare(o TableMetadata) int {
	if c := strings.Compare(tm.Schema, o.Schema); c != 0 {
		return c
	}
	return strings.Compare(tm.Table, o.Table)
}

func (tm TableMetadata) Less(o TableMetadata) bool {
	return tm.Compare(o) < 0
}

// Verify verifies the given connections have matching tables and contents.
func Verify(ctx context.Context, conns []Conn, reporters ...Reporter) error {
	reportAll := CombinedReporter{Reporters: reporters}
	defer reportAll.Close()

	ret, err := verifyDatabaseTables(ctx, conns)
	if err != nil {
		return errors.Wrap(err, "error comparing conns")
	}

	for _, missingTable := range ret.missingTables {
		reportAll.Report(missingTable)
	}
	for _, extraneousTable := range ret.extraneousTables {
		reportAll.Report(extraneousTable)
	}

	// Grab columns for each table on both sides.
	tbls, err := verifyCommonTables(ctx, conns, ret.verified)
	if err != nil {
		return err
	}

	g := ctxgroup.WithContext(ctx)
	for _, tbl := range tbls {
		tbl := tbl

		for _, d := range tbl.MismatchingTableDefinitions {
			reportAll.Report(d)
		}
		// TODO: limit tables to compare at a time.
		g.GoCtx(func(ctx context.Context) error {
			connsCopy := make([]Conn, len(conns))
			copy(connsCopy, conns)
			for i := range connsCopy {
				i := i
				connsCopy[i].Conn, err = pgx.ConnectConfig(ctx, conns[i].Conn.Config())
				if err != nil {
					return errors.Wrap(err, "error establishing connection to compare")
				}
				defer func() {
					_ = connsCopy[i].Conn.Close(ctx)
				}()
			}
			return compareRows(ctx, connsCopy, tbl)
		})
	}
	return g.Wait()
}
