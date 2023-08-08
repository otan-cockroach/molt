package datamove

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/datamove/dataquery"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/cockroachdb/molt/retry"
	"github.com/rs/zerolog"
)

type importResult struct {
	StartTime time.Time
	EndTime   time.Time
}

func importTable(
	ctx context.Context,
	baseConn dbconn.Conn,
	logger zerolog.Logger,
	table dbtable.VerifiedTable,
	resources []datamovestore.Resource,
) (importResult, error) {
	ret := importResult{
		StartTime: time.Now(),
	}

	var locs []string
	for _, resource := range resources {
		u, err := resource.ImportURL()
		if err != nil {
			return importResult{}, err
		}
		locs = append(locs, u)
	}
	conn := baseConn.(*dbconn.PGConn)
	r, err := retry.NewRetry(retry.Settings{
		InitialBackoff: time.Second,
		Multiplier:     2,
		MaxRetries:     4,
	})
	if err != nil {
		return ret, err
	}
	if err := r.Do(func() error {
		if _, err := conn.Exec(
			ctx,
			dataquery.ImportInto(table, locs),
		); err != nil {
			return errors.Wrap(err, "error importing data")
		}
		return nil
	}, func(err error) {
		logger.Err(err).Msgf("error importing data, retrying")
	}); err != nil {
		return ret, err
	}
	ret.EndTime = time.Now()
	return ret, nil
}
