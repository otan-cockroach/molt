package parsectx

import (
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/duration"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/timeutil/pgdate"
)

type parseContext struct{}

var _ tree.ParseContext = (*parseContext)(nil)

func (p parseContext) GetCollationEnv() *tree.CollationEnvironment {
	return nil
}

func (p parseContext) GetDateHelper() *pgdate.ParseHelper {
	return nil
}

func (p parseContext) GetRelativeParseTime() time.Time {
	return time.Now().UTC()
}

func (p parseContext) GetIntervalStyle() duration.IntervalStyle {
	return duration.IntervalStyle_POSTGRES
}

func (p parseContext) GetDateStyle() pgdate.DateStyle {
	return pgdate.DefaultDateStyle()
}

var ParseContext = &parseContext{}
