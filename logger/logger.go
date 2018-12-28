package logger

import (
	"bytes"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

var (
	gray    = string([]byte{0x1b, 0x5b, 0x31, 0x3b, 0x33, 0x37, 0x6d})
	green   = string([]byte{27, 91, 57, 55, 59, 52, 50, 109})
	white   = string([]byte{27, 91, 57, 48, 59, 52, 55, 109})
	yellow  = string([]byte{27, 91, 57, 55, 59, 52, 51, 109})
	red     = string([]byte{27, 91, 57, 55, 59, 52, 49, 109})
	blue    = string([]byte{27, 91, 57, 55, 59, 52, 52, 109})
	magenta = string([]byte{27, 91, 57, 55, 59, 52, 53, 109})
	cyan    = string([]byte{27, 91, 57, 55, 59, 52, 54, 109})
	reset   = string([]byte{27, 91, 48, 109})

	fg_yellow  = string([]byte{27, 91, 57, 51, 59, 52, 48, 109})
	fg_cyan    = string([]byte{27, 91, 57, 54, 59, 52, 48, 109})
	fg_magenta = string([]byte{27, 91, 57, 53, 59, 52, 48, 109})
)

var reNonPrintable *regexp.Regexp

func init() {
	reNonPrintable = regexp.MustCompile(`[^[:print:]]`)
}

func Simple(query string, params []interface{}, d time.Duration, rows int64, err error) {
	if len(params) > 0 {
		log.Printf("%s %s", query, PrettyParams(params))
	} else {
		log.Print(query)
	}
}

func Plain(query string, params []interface{}, dur time.Duration, rows int64, err error) {
	paramstr := ""
	durstr := ""
	rowstr := ""
	errstr := ""

	if len(params) > 0 {
		paramstr = " " + PrettyParams(params)
	}

	if dur > 0 {
		durstr = fmt.Sprintf(" %v", dur)
	}

	if rows >= 0 {
		rowstr = fmt.Sprintf(" %d rows", rows)
	}

	if err != nil {
		errstr = " => " + err.Error()
	}

	log.Printf("%s%s%s%s%s", query, paramstr, durstr, rowstr, errstr)
}

func Color(query string, params []interface{}, dur time.Duration, rows int64, err error) {
	color := gray
	paramstr := ""
	durstr := ""
	rowstr := ""
	errstr := ""

	if strings.HasPrefix(query, "INSERT ") {
		color = fg_yellow
	} else if strings.HasPrefix(query, "UPDATE ") {
		color = fg_cyan
	} else if strings.HasPrefix(query, "DELETE ") {
		color = fg_magenta
	}

	if len(params) > 0 {
		paramstr = " " + PrettyParams(params)
	}

	if dur > 0 {
		durstr = fmt.Sprintf(" %v", dur)
	}

	if rows >= 0 {
		rowstr = fmt.Sprintf(" %d rows", rows)
	}

	if err != nil {
		color = red
		errstr = " => " + err.Error()
	}

	log.Printf("%s%s%s%s%s%s%s", color, query, reset, paramstr, durstr, rowstr, errstr)
}

func PrettyParams(a []interface{}) string {
	maxlen := 100
	var b bytes.Buffer
	b.WriteString("[")
	for i, e := range a {
		if i > 0 {
			b.WriteString(" ")
		}
		if s, ok := e.(string); ok {
			if len(s) > maxlen {
				s = s[:maxlen] + "..."
			}
			b.WriteString("`" + s + "`")
		} else if ps, ok := e.(*string); ok {
			if ps != nil {
				s := *ps
				if len(s) > maxlen {
					s = s[:maxlen] + "..."
				}
				b.WriteString("`" + s + "`")
			} else {
				b.WriteString("nil")
			}
		} else if v, ok := e.([]byte); ok {
			s := string(v)
			if len(s) > maxlen {
				s = s[:maxlen] + "..."
			}
			b.WriteString("`" + reNonPrintable.ReplaceAllLiteralString(s, "?") + "`")
		} else {
			b.WriteString(fmt.Sprintf("%#v", e))
		}
	}
	b.WriteString("]")
	return b.String()
}
