package verification

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/molt/retry"
	"github.com/stretchr/testify/require"
)

func TestReverifyQueue(t *testing.T) {
	const numItems = 5
	retryItems := make([]*RetryItem, numItems)
	for i := 0; i < numItems; i++ {
		r, err := retry.NewRetryWithTime(
			time.Date(2020, 12, 30, 0, 50, 30, 0, time.UTC).Add(-time.Duration(i)*time.Hour),
			retry.DefaultSettings(),
		)
		require.NoError(t, err)
		retryItems[i] = &RetryItem{
			PrimaryKeys: []tree.Datums{{tree.NewDString(fmt.Sprintf("%d", i+1))}},
			Retry:       r,
		}
	}
	var q reverifyQueue
	for _, it := range retryItems {
		q.heapPush(it)
	}
	for i := numItems - 1; i >= 0; i-- {
		require.Equal(t, retryItems[i], q[0])
		it := q.heapPop()
		require.Equal(t, retryItems[i], it)
	}
	q.heapPush(retryItems[2])
	q.heapPush(retryItems[4])
	require.Equal(t, retryItems[4], q.heapPop())
	q.heapPush(retryItems[1])
	require.Equal(t, retryItems[2], q.heapPop())
	require.Equal(t, retryItems[1], q.heapPop())
}
