package datamove

import (
	"context"
	"time"

	"github.com/cockroachdb/molt/datamove/datamovestore"
	"github.com/cockroachdb/molt/datamove/dataquery"
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/dbtable"
	"github.com/rs/zerolog"
)

type CopyResult struct {
	StartTime time.Time
	EndTime   time.Time
}

func Copy(
	ctx context.Context,
	baseConn dbconn.Conn,
	logger zerolog.Logger,
	table dbtable.VerifiedTable,
	resources []datamovestore.Resource,
) (CopyResult, error) {
	ret := CopyResult{
		StartTime: time.Now(),
	}

	conn := baseConn.(*dbconn.PGConn).Conn

	for i, resource := range resources {
		logger.Debug().
			Int("idx", i+1).
			Msgf("reading resource")
		if err := func() error {
			r, err := resource.Reader(ctx)
			if err != nil {
				return err
			}
			logger.Debug().
				Int("idx", i+1).
				Msgf("running copy from resource")
			if _, err := conn.PgConn().CopyFrom(
				ctx,
				r,
				dataquery.CopyFrom(table),
			); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return ret, err
		}
	}

	ret.EndTime = time.Now()
	logger.Info().
		Dur("duration", ret.EndTime.Sub(ret.StartTime)).
		Msgf("table COPY complete")
	return ret, nil
}
