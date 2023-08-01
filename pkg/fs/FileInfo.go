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

type FileInfo interface {
	IsDir() bool
	Name() string
	ModTime() time.Time
	Size() int64
	String() string
}
