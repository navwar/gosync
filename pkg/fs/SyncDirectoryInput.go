// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

type SyncDirectoryInput struct {
	SourceDirectory       string
	SourceFileSystem      FileSystem
	DestinationDirectory  string
	DestinationFileSystem FileSystem
	CheckTimestamps       bool
	Limit                 int
	Logger                Logger
	MaxThreads            int
}
