package verification

import (
	"context"
	"fmt"
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

type OID uint32

func (tm tableMetadata) Compare(o tableMetadata) int {
	if c := strings.Compare(tm.Schema, o.Schema); c != 0 {
		return c
	}
	return strings.Compare(tm.Table, o.Table)
}

func (tm tableMetadata) Less(o tableMetadata) bool {
	return tm.Compare(o) < 0
}

// Verify verifies the given connections have matching tables and contents.
func Verify(ctx context.Context, conns []Conn) error {
	ret, err := verifyDatabaseTables(ctx, conns)
	if err != nil {
		return errors.Wrap(err, "error comparing conns")
	}

	for _, missingTable := range ret.missingTables {
		log.Printf("[%s] missing table %s.%s", missingTable.connID, missingTable.Schema, missingTable.Table)
	}
	for _, extraneousTable := range ret.extraneousTables {
		log.Printf("[%s] extraneous table %s.%s", extraneousTable.connID, extraneousTable.Schema, extraneousTable.Table)
	}

	// Grab columns for each table on both sides.
	comparableTable, err := verifyCommonTables(ctx, conns, ret.verified)
	if err != nil {
		return err
	}

	fmt.Printf("%#v\n", comparableTable)
	g := ctxgroup.WithContext(ctx)
	for _, tbl := range comparableTable {
		tbl := tbl
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
