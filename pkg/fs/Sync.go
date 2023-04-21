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
	"time"
)

func Sync(ctx context.Context, input *SyncInput) (int, error) {

	if input.Logger != nil {
		input.Logger.Log("Synchronizing", map[string]interface{}{
			"src":     input.Source,
			"dst":     input.Destination,
			"threads": input.MaxThreads,
		})
	}

	sourceFileInfo, err := input.SourceFileSystem.Stat(ctx, input.Source)
	if err != nil {
		if input.SourceFileSystem.IsNotExist(err) {
			return 0, fmt.Errorf("source does not exist %q: %w", input.Source, err)
		}
		return 0, fmt.Errorf("error stating source %q: %w", input.Source, err)
	}

	// if source is a directory
	if sourceFileInfo.IsDir() {
		if _, err := input.DestinationFileSystem.Stat(ctx, input.Destination); err != nil {
			if input.DestinationFileSystem.IsNotExist(err) {
				if !input.Parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", input.Destination)
				}
			}
		}
		count, err := SyncDirectory(ctx, &SyncDirectoryInput{
			SourceDirectory:       input.Source,
			SourceFileSystem:      input.SourceFileSystem,
			DestinationDirectory:  input.Destination,
			DestinationFileSystem: input.DestinationFileSystem,
			CheckTimestamps:       input.CheckTimestamps,
			Limit:                 input.Limit,
			Logger:                input.Logger,
			MaxThreads:            input.MaxThreads,
		})
		if err != nil {
			return 0, fmt.Errorf(
				"error syncing source directory %q (base %q) to destination directory %q (base %q): %w",
				input.Source,
				input.SourceFileSystem.Root(),
				input.Destination,
				input.DestinationFileSystem.Root(),
				err,
			)
		}
		return count, nil
	}

	// if source is a file
	copyFile := false

	destinationFileInfo, err := input.DestinationFileSystem.Stat(ctx, input.Destination)
	if err != nil {
		if input.DestinationFileSystem.IsNotExist(err) {
			copyFile = true
		} else {
			return 0, fmt.Errorf("error stating destination %q: %w", input.Destination, err)
		}
	} else {
		if sourceFileInfo.Size() != destinationFileInfo.Size() {
			copyFile = true
		}
		if input.CheckTimestamps {
			if !EqualTimestamp(sourceFileInfo.ModTime(), destinationFileInfo.ModTime(), time.Second) {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = Copy(ctx, &CopyInput{
			SourceName:            input.Source,
			SourceFileSystem:      input.SourceFileSystem,
			DestinationName:       input.Destination,
			DestinationFileSystem: input.DestinationFileSystem,
			Parents:               input.Parents,
			Logger:                input.Logger,
		})
		if err != nil {
			return 0, fmt.Errorf("error copying %q to %q: %w", input.Source, input.Destination, err)
		}
		return 1, nil
	}

	return 0, nil
}
