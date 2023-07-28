package datamove

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
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
	bucket string,
	table dbtable.Name,
) (ImportResult, error) {
	ret := ImportResult{
		StartTime: time.Now(),
	}
	sess := session.Must(session.NewSession())

	conn := baseConn.(*dbconn.PGConn)
	creds, err := sess.Config.Credentials.Get()
	if err != nil {
		return ret, err
	}
	loc := fmt.Sprintf(
		"s3://%s/%s.dump?AWS_ACCESS_KEY_ID=%s&AWS_SECRET_ACCESS_KEY=%s",
		bucket,
		table.SafeString(),
		creds.AccessKeyID,
		creds.SecretAccessKey,
	)
	if _, err := conn.Exec(ctx, "IMPORT INTO "+table.SafeString()+" CSV DATA ('"+loc+"')"); err != nil {
		return ret, err
	}
	ret.EndTime = time.Now()
	logger.Debug().
		Dur("duration", ret.EndTime.Sub(ret.StartTime)).
		Msgf("table imported")
	return ret, nil
}
