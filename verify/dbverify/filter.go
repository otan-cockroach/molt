package dbverify

import (
	"regexp"

	"github.com/cockroachdb/molt/dbtable"
)

const DefaultFilterString = ".*"

type FilterString = string

func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		SchemaFilter: DefaultFilterString,
		TableFilter:  DefaultFilterString,
	}
}

type FilterConfig struct {
	SchemaFilter FilterString
	TableFilter  FilterString
}

func FilterResult(cfg FilterConfig, r Result) (Result, error) {
	if cfg.SchemaFilter == DefaultFilterString && cfg.TableFilter == DefaultFilterString {
		return r, nil
	}
	schemaRe, err := regexp.CompilePOSIX(cfg.SchemaFilter)
	if err != nil {
		return r, err
	}
	tableRe, err := regexp.CompilePOSIX(cfg.TableFilter)
	if err != nil {
		return r, err
	}
	newResult := Result{
		Verified:         r.Verified[:0],
		MissingTables:    r.MissingTables[:0],
		ExtraneousTables: r.ExtraneousTables[:0],
	}
	for _, v := range r.Verified {
		if matchesFilter(v[0].Name, schemaRe, tableRe) {
			newResult.Verified = append(newResult.Verified, v)
		}
	}
	for _, t := range r.MissingTables {
		if matchesFilter(t.Name, schemaRe, tableRe) {
			newResult.MissingTables = append(newResult.MissingTables, t)
		}
	}
	for _, t := range r.ExtraneousTables {
		if matchesFilter(t.Name, schemaRe, tableRe) {
			newResult.ExtraneousTables = append(newResult.ExtraneousTables, t)
		}
	}
	return newResult, nil
}

func matchesFilter(n dbtable.Name, schemaRe, tableRe *regexp.Regexp) bool {
	return schemaRe.MatchString(string(n.Schema)) && tableRe.MatchString(string(n.Table))
}
