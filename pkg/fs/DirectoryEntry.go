// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

import "time"

type DirectoryEntry interface {
	IsDir() bool
	MarshalJSON() ([]byte, error)
	ModTime() time.Time
	Name() string
	Size() int64
	String() string
}
