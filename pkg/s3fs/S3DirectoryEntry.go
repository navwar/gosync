// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package s3fs

import (
	"encoding/json"
	"time"
)

type S3DirectoryEntry struct {
	name    string
	dir     bool
	modTime time.Time
	size    int64
}

func (de *S3DirectoryEntry) IsDir() bool {
	return de.dir
}

func (de *S3DirectoryEntry) Name() string {
	return de.name
}

func (de *S3DirectoryEntry) ModTime() time.Time {
	return de.modTime
}

func (de *S3DirectoryEntry) Size() int64 {
	return de.size
}

func (de *S3DirectoryEntry) String() string {
	return de.name
}

func (de *S3DirectoryEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"dir":     de.dir,
		"modTime": de.modTime,
		"name":    de.name,
		"size":    de.size,
	})
}
