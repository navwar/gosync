// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package fs

import (
	"context"
	"fmt"
	"strings"
)

func Sync(ctx context.Context, input *SyncInput) (int, error) {

	if !strings.HasPrefix(input.Source, "/") {
		return 0, fmt.Errorf("source %q does not start with /", input.Source)
	}

	if !strings.HasPrefix(input.Destination, "/") {
		return 0, fmt.Errorf("destination %q does not start with /", input.Destination)
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Synchronizing", map[string]interface{}{
			"delete":  input.Delete,
			"dst":     input.Destination,
			"exclude": input.Exclude,
			"threads": input.MaxThreads,
			"src":     input.Source,
		})
	}

	skip := false
	if len(input.Exclude) > 0 {
		for _, exclude := range input.Exclude {
			if strings.HasPrefix(exclude, "*") {
				if strings.HasSuffix(exclude, "*") {
					if strings.Contains(input.Source, exclude[1:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if strings.HasSuffix(input.Source, exclude[1:]) {
						skip = true
						break
					}
				}
			} else {
				if strings.HasSuffix(exclude, "*") {
					if strings.HasPrefix(input.Source, exclude[0:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if input.Source == exclude {
						skip = true
						break
					}
				}
			}
		}
	}

	if skip {
		if input.Logger != nil {
			_ = input.Logger.Log("Skipping source", map[string]interface{}{
				"src": input.Source,
			})
		}
		return 0, nil
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
		if _, statError := input.DestinationFileSystem.Stat(ctx, input.Destination); statError != nil {
			if input.DestinationFileSystem.IsNotExist(statError) {
				if !input.Parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", input.Destination)
				}
			}
		}
		count, syncDirectoryError := SyncDirectory(ctx, &SyncDirectoryInput{
			Delete:                input.Delete,
			CheckTimestamps:       input.CheckTimestamps,
			SourceDirectory:       input.Source,
			SourceFileSystem:      input.SourceFileSystem,
			DestinationDirectory:  input.Destination,
			DestinationFileSystem: input.DestinationFileSystem,
			Exclude:               input.Exclude,
			Limit:                 input.Limit,
			Logger:                input.Logger,
			MaxThreads:            input.MaxThreads,
			TimestampPrecision:    input.TimestampPrecision,
		})
		if syncDirectoryError != nil {
			return 0, fmt.Errorf(
				"error syncing source directory %q (base %q) to destination directory %q (base %q): %w",
				input.Source,
				input.SourceFileSystem.Root(),
				input.Destination,
				input.DestinationFileSystem.Root(),
				syncDirectoryError,
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
			if !EqualTimestamp(sourceFileInfo.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = Copy(ctx, &CopyInput{
			CheckParents:          true, // check if parent for destination exists
			SourceName:            input.Source,
			SourceFileSystem:      input.SourceFileSystem,
			DestinationName:       input.Destination,
			DestinationFileSystem: input.DestinationFileSystem,
			Logger:                input.Logger,
			MakeParents:           input.Parents, // create destination parents if not exist
		})
		if err != nil {
			return 0, fmt.Errorf("error copying %q to %q: %w", input.Source, input.Destination, err)
		}
		return 1, nil
	} else {
		if input.Logger != nil {
			_ = input.Logger.Log("Skipping file", map[string]interface{}{
				"src": input.Source,
			})
		}
	}

	return 0, nil
}
