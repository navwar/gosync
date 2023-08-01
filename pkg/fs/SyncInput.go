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

type SyncInput struct {
	Delete                bool
	Source                string // could be file or directory
	SourceFileSystem      FileSystem
	Destination           string // could be file or directory
	Exclude               []string
	DestinationFileSystem FileSystem
	Parents               bool
	CheckTimestamps       bool
	Limit                 int
	Logger                Logger
	MaxThreads            int
	TimestampPrecision    time.Duration
}
