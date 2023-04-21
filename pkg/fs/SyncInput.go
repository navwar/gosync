// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

type SyncInput struct {
	Source                string // could be file or directory
	SourceFileSystem      FileSystem
	Destination           string // could be file or directory
	DestinationFileSystem FileSystem
	Parents               bool
	CheckTimestamps       bool
	Limit                 int
	Logger                Logger
	MaxThreads            int
}
