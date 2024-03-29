// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"time"
)

type LocalFileInfo struct {
	name string
	size int64
	//mode fs.FileMode
	modTime time.Time
	dir     bool
	//sys any
}

func (fi *LocalFileInfo) IsDir() bool {
	return fi.dir
}

func (fi *LocalFileInfo) Name() string {
	return fi.name
}

func (fi *LocalFileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *LocalFileInfo) Size() int64 {
	return fi.size
}

func (fi *LocalFileInfo) String() string {
	return fi.name
}

func NewLocalFileInfo(name string, modTime time.Time, dir bool, size int64) *LocalFileInfo {
	return &LocalFileInfo{
		name:    name,
		modTime: modTime,
		dir:     dir,
		size:    size,
	}
}
