// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

type CopyInput struct {
	CheckParents          bool
	SourceName            string
	SourceFileInfo        FileInfo
	SourceFileSystem      FileSystem
	DestinationName       string
	DestinationFileInfo   FileInfo
	DestinationFileSystem FileSystem
	Logger                Logger
	MakeParents           bool
}
