package datamove

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	table dbtable.Name,
	files []string,
) (ImportResult, error) {
	ret := ImportResult{
		StartTime: time.Now(),
	}

	var locs []string
	for _, file := range files {
		locs = append(
			locs,
			fmt.Sprintf("'%s'", file),
		)
	}
	//s3manager.NewDownloader(session.Must(session.NewSession())).DownloadWithContext(ctx, w, s3.GetObjectInput{})
	conn := baseConn.(*dbconn.PGConn)
	if _, err := conn.Exec(
		ctx,
		"IMPORT INTO "+table.SafeString()+" CSV DATA ("+strings.Join(locs, ",")+")",
	); err != nil {
		return ret, err
	}
	ret.EndTime = time.Now()
	logger.Info().
		Dur("duration", ret.EndTime.Sub(ret.StartTime)).
		Msgf("table imported")
	return ret, nil
}
