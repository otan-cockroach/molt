package comparectx

import (
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// compareContext implements tree.CompareContext
type compareContext struct{}

func (c *compareContext) UnwrapDatum(d tree.Datum) tree.Datum {
	return d
}

func (c *compareContext) GetLocation() *time.Location {
	return time.UTC
}

func (c *compareContext) GetRelativeParseTime() time.Time {
	return time.Now().UTC()
}

func (c *compareContext) MustGetPlaceholderValue(p *tree.Placeholder) tree.Datum {
	return p
}

var CompareContext = &compareContext{}
