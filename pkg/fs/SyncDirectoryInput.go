// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

type SyncDirectoryInput struct {
	CheckTimestamps       bool
	DestinationDirectory  string
	DestinationFileSystem FileSystem
	Exclude               []string
	Limit                 int
	Logger                Logger
	MaxThreads            int
	SourceDirectory       string
	SourceFileSystem      FileSystem
}
