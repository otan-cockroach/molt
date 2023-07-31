package datamovestore

import (
	"context"
	"io"

	"github.com/cockroachdb/molt/datamove/dataquery"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

// copyCRDBDirect represents a store in which any output is directly input
// into CockroachDB, instead of storing it as an intermediate file.
// This is only compatible with "COPY", and does not utilise IMPORT.
type copyCRDBDirect struct {
	logger zerolog.Logger
	target *pgx.Conn
}

func (c *copyCRDBDirect) CreateFromReader(
	ctx context.Context, r io.Reader, table dbtable.VerifiedTable, iteration int,
) (Resource, error) {
	c.logger.Debug().Int("batch", iteration).Msgf("csv batch starting")
	if _, err := c.target.PgConn().CopyFrom(ctx, r, dataquery.CopyFrom(table)); err != nil {
		return nil, err
	}
	c.logger.Debug().Int("batch", iteration).Msgf("csv batch complete")
	return nil, nil
}

func (c *copyCRDBDirect) CanBeTarget() bool {
	return false
}

func (c *copyCRDBDirect) DefaultFlushBatchSize() int {
	return 1 * 1024 * 1024
}

func (c *copyCRDBDirect) Cleanup(ctx context.Context) error {
	return nil
}

func NewCopyCRDBDirect(logger zerolog.Logger, target *pgx.Conn) *copyCRDBDirect {
	return &copyCRDBDirect{
		logger: logger,
		target: target,
	}
}
