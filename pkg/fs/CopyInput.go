// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

type CopyInput struct {
	SourceName            string
	SourceFileSystem      FileSystem
	DestinationName       string
	DestinationFileSystem FileSystem
	Parents               bool
	Logger                Logger
}
