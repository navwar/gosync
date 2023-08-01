// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"encoding/json"
	"io/fs"
	"time"
)

type LocalDirectoryEntry struct {
	de fs.DirEntry
}

func (lde *LocalDirectoryEntry) IsDir() bool {
	return lde.de.IsDir()
}

func (lde *LocalDirectoryEntry) Name() string {
	return lde.de.Name()
}

func (lde *LocalDirectoryEntry) ModTime() time.Time {
	if i, err := lde.de.Info(); err == nil {
		return i.ModTime()
	}
	return time.Time{}
}

func (lde *LocalDirectoryEntry) Size() int64 {
	if i, err := lde.de.Info(); err == nil {
		return i.Size()
	}
	return -1
}

func (lde *LocalDirectoryEntry) String() string {
	return lde.de.Name()
}

func (lde *LocalDirectoryEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"dir":     lde.IsDir(),
		"modTime": lde.ModTime(),
		"name":    lde.Name(),
		"size":    lde.Size(),
	})
}
