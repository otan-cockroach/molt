package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/pkg/verification"
	"github.com/jackc/pgx/v5"
)

var flagPGURL = flag.String(
	"pg_url",
	"postgres://otan@localhost:5432/otan",
	"postgresql url",
)
var flagCRDBURL = flag.String(
	"crdb_url",
	"postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable",
	"cockroachdb url",
)

func main() {
	flag.Parse()

	reporter := &verification.LogReporter{Printf: log.Printf}
	defer reporter.Close()

	ctx := context.Background()
	var conns []verification.Conn
	for _, c := range []struct {
		id    verification.ConnID
		pgurl string
	}{
		{id: "postgres", pgurl: *flagPGURL},
		{id: "cockroach", pgurl: *flagCRDBURL},
	} {
		conn, err := pgx.Connect(ctx, c.pgurl)
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error connecting to %s", c.pgurl))
		}
		conns = append(conns, verification.Conn{ID: c.id, Conn: conn})
		reporter.Report(verification.StatusReport{Info: fmt.Sprintf("connected to %s", c.id)})
	}

	reporter.Report(verification.StatusReport{Info: "verification in progress"})
	if err := verification.Verify(
		ctx,
		conns,
		reporter,
	); err != nil {
		log.Fatal(errors.Wrapf(err, "error verifying"))
	}
	reporter.Report(verification.StatusReport{Info: "verification complete"})
}
