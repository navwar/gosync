// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

import "time"

type DirectoryEntryInterface interface {
	IsDir() bool
	MarshalJSON() ([]byte, error)
	ModTime() time.Time
	Name() string
	Size() int64
	String() string
}
