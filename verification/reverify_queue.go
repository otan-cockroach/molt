package verification

import (
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

type QueueItem struct {
	Schema                 tree.Name
	Table                  tree.Name
	MatchingColumns        []tree.Name
	MatchingColumnTypeOIDs [2][]oid.Oid
	PrimaryKeyColumns      []tree.Name

	PrimaryKeys []tree.Datums
	Retry       RetryMetadata
}

type RetrySettings struct {
	InitialBackoff time.Duration
	Multiplier     int
	MaxBackoff     time.Duration
	MaxRetries     int
}

type RetryMetadata struct {
	Iteration  int
	FirstRetry time.Time
	NextRetry  time.Time

	settings RetrySettings
}
