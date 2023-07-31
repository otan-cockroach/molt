package datamove

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/datamove/dataquery"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
)

type ImportResult struct {
	StartTime time.Time
	EndTime   time.Time
}

func Import(
	ctx context.Context,
	baseConn dbconn.Conn,
	logger zerolog.Logger,
	table dbtable.VerifiedTable,
	resources []datamovestore.Resource,
) (ImportResult, error) {
	ret := ImportResult{
		StartTime: time.Now(),
	}

	var locs []string
	for _, resource := range resources {
		u, err := resource.ImportURL()
		if err != nil {
			return ImportResult{}, err
		}
		locs = append(locs, u)
	}
	conn := baseConn.(*dbconn.PGConn)
	if _, err := conn.Exec(
		ctx,
		dataquery.ImportInto(table, locs),
	); err != nil {
		return ret, errors.Wrap(err, "error importing data")
	}
	ret.EndTime = time.Now()
	logger.Info().
		Dur("duration", ret.EndTime.Sub(ret.StartTime)).
		Msgf("table imported")
	return ret, nil
}
