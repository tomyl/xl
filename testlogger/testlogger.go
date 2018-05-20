package testlogger

import (
	"testing"
	"time"

	"github.com/tomyl/xl/logger"
)

func Simple(t *testing.T) func(string, []interface{}, time.Duration, int64, error) {
	return func(query string, params []interface{}, d time.Duration, rows int64, err error) {
		if len(params) > 0 {
			t.Logf("%s %d %v %s", query, rows, d, logger.PrettyParams(params))
		} else {
			t.Logf("%s %d %v", query, rows, d)
		}
	}
}
