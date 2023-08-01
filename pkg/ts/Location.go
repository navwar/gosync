// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package ts

import (
	"errors"
	"strconv"
	"time"
)

func ParseLocation(location string) (*time.Location, error) {
	if location == "" {
		return nil, errors.New("cannot parse location from empty string")
	}
	if location == "Local" {
		return time.Local, nil
	}
	hours, err := strconv.Atoi(location)
	if err == nil {
		return time.FixedZone("UTC"+location, hours*60*60), nil
	}
	return time.LoadLocation(location)
}
