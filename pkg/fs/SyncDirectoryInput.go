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

type SyncDirectoryInput struct {
	Delete                bool
	CheckTimestamps       bool
	DestinationDirectory  string
	DestinationFileSystem FileSystem
	Exclude               []string
	Limit                 int
	Logger                Logger
	MaxThreads            int
	SourceDirectory       string
	SourceFileSystem      FileSystem
	TimestampPrecision    time.Duration
}
