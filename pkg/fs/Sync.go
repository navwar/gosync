// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package fs

import (
	"context"
	"fmt"
)

func Sync(ctx context.Context, source string, sourceFileSystem FileSystem, destination string, destinationFileSystem FileSystem, parents bool, checkTimestamps bool, limit int, logger Logger) (int, error) {

	sourceFileInfo, err := sourceFileSystem.Stat(ctx, source)
	if err != nil {
		if sourceFileSystem.IsNotExist(err) {
			return 0, fmt.Errorf("source does not exist %q: %w", source, err)
		}
	}

	// if source is a directory
	if sourceFileInfo.IsDir() {
		if _, err := destinationFileSystem.Stat(ctx, destination); err != nil {
			if destinationFileSystem.IsNotExist(err) {
				if !parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", destination)
				}
			}
		}
		count, err := SyncDirectory(ctx, source, sourceFileSystem, destination, destinationFileSystem, checkTimestamps, limit, logger)
		if err != nil {
			return 0, fmt.Errorf("error syncing source directory %q to destination directory %q: %w", source, destination, err)
		}
		return count, nil
	}

	// if source is a file
	copyFile := false

	destinationFileInfo, err := destinationFileSystem.Stat(ctx, destination)
	if err != nil {
		if destinationFileSystem.IsNotExist(err) {
			copyFile = true
		} else {
			return 0, fmt.Errorf("error stating destination %q: %w", destination, err)
		}
	} else {
		if sourceFileInfo.Size() != destinationFileInfo.Size() {
			copyFile = true
		}
		if checkTimestamps {
			if sourceFileInfo.ModTime() != destinationFileInfo.ModTime() {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = Copy(ctx, source, sourceFileSystem, destination, destinationFileSystem, parents, logger)
		if err != nil {
			return 0, fmt.Errorf("error copying %q to %q: %w", source, destination, err)
		}
		return 1, nil
	}

	return 0, nil
}
