// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package ts

import "time"

// Layout is a string that describes the text representation of a time
type Layout string

func (l Layout) Format(t time.Time) string {
	return t.Format(string(l))
}

// NamedLayouts includes a map of layouts that can be referenced by name
var NamedLayouts = map[string]Layout{
	"Kitchen":     time.Kitchen,
	"RFC3339":     time.RFC3339,
	"RFC3339Nano": time.RFC3339Nano,
	"DateTime":    time.DateTime,
	"DateOnly":    time.DateOnly,
	"TimeOnly":    time.TimeOnly,
	"Default":     "Jan 02 15:04",
	"Full":        "Jan 02 15:04:05 2006",
}

// ParseLayout returns a layout.
// If layout is the name of a known layout, then returns the referenced layout.
// Otherwise, returns the input layout.
func ParseLayout(layout string) Layout {
	format, ok := NamedLayouts[layout]
	if ok {
		return format
	}
	return Layout(layout)
}
