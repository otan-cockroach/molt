package datamovestore

import (
	"context"
	"io"

	"github.com/cockroachdb/molt/dbtable"
)

type Store interface {
	CreateFromReader(ctx context.Context, r io.Reader, table dbtable.Name, iteration int) (string, error)
	CanBeTarget() bool
	DefaultFlushBatchSize() int
}
