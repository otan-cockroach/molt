package datamovestore

import (
	"context"
	"io"

	"github.com/cockroachdb/molt/dbtable"
)

type Store interface {
	CreateFromReader(ctx context.Context, r io.Reader, table dbtable.Name, iteration int) (Resource, error)
	CanBeTarget() bool
	DefaultFlushBatchSize() int
}

type Resource interface {
	ImportURL() (string, error)
	Cleanup(ctx context.Context) error
}
