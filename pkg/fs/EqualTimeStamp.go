// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

import (
	"time"
)

// EqualTimestamp returns true if time a and b are equal after being truncated.
// When syncing between a source and destination filesystem with different precision for their last modified timestamps,
// this function can be used to confirm the stored timestamps are equivalent.
func EqualTimestamp(a time.Time, b time.Time, d time.Duration) bool {
	return a.Truncate(d).Equal(b.Truncate(d))
}
