// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

import (
	"time"
)

func EqualTimestamp(a time.Time, b time.Time, d time.Duration) bool {
	return a.Truncate(d).Equal(b.Truncate(d))
}
