// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

import (
	"encoding/json"
	"time"
)

type DirectoryEntry struct {
	name    string
	dir     bool
	modTime time.Time
	size    int64
}

func (de *DirectoryEntry) IsDir() bool {
	return de.dir
}

func (de *DirectoryEntry) Name() string {
	return de.name
}

func (de *DirectoryEntry) ModTime() time.Time {
	return de.modTime
}

func (de *DirectoryEntry) Size() int64 {
	return de.size
}

func (de *DirectoryEntry) String() string {
	return de.name
}

func (de *DirectoryEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"dir":     de.dir,
		"modTime": de.modTime,
		"name":    de.name,
		"size":    de.size,
	})
}

func NewDirectoryEntry(name string, dir bool, modTime time.Time, size int64) *DirectoryEntry {
	return &DirectoryEntry{
		name:    name,
		dir:     dir,
		modTime: modTime,
		size:    size,
	}
}
