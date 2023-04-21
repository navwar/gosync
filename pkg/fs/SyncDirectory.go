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
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

func SyncDirectory(ctx context.Context, input *SyncDirectoryInput) (int, error) {

	// limit is zero
	if input.Limit == 0 {
		return 0, nil
	}

	sourceDirectoryEntries, err := input.SourceFileSystem.ReadDir(ctx, input.SourceDirectory)
	if err != nil {
		return 0, fmt.Errorf("error reading source directory %q: %w", input.SourceDirectory, err)
	}

	if input.Logger != nil {
		input.Logger.Log("Synchronizing Directory", map[string]interface{}{
			"src":   input.SourceDirectory,
			"dst":   input.DestinationDirectory,
			"files": len(sourceDirectoryEntries),
		})
	}

	// wait group
	var wg errgroup.Group

	// If input.MaxThreads is greater than zero, than set limit.
	if input.MaxThreads > 0 {
		wg.SetLimit(input.MaxThreads)
	}

	// declare count
	count := 0

	for _, sourceDirectoryEntry := range sourceDirectoryEntries {
		if sourceDirectoryEntry.Name() == "" {
			return 0, fmt.Errorf("source directory name is empty when reading %q", input.SourceDirectory)
		}
		sourceDirectoryEntry := sourceDirectoryEntry
		sourceName := filepath.Join(input.SourceDirectory, sourceDirectoryEntry.Name())
		destinationName := filepath.Join(input.DestinationDirectory, sourceDirectoryEntry.Name())
		if sourceDirectoryEntry.IsDir() {
			// synchronize directory and wait until all files are finished copying
			directoryLimit := -1
			if input.Limit != -1 {
				directoryLimit = input.Limit - count
			}
			c, err := SyncDirectory(ctx, &SyncDirectoryInput{
				SourceDirectory:       sourceName,
				SourceFileSystem:      input.SourceFileSystem,
				DestinationDirectory:  destinationName,
				DestinationFileSystem: input.DestinationFileSystem,
				CheckTimestamps:       input.CheckTimestamps,
				Limit:                 directoryLimit,
				Logger:                input.Logger,
				MaxThreads:            input.MaxThreads,
			})
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			count += 1
			wg.Go(func() error {
				copyFile := false
				destinationFileInfo, err := input.DestinationFileSystem.Stat(ctx, destinationName)
				if err != nil {
					if input.DestinationFileSystem.IsNotExist(err) {
						copyFile = true
					} else {
						return fmt.Errorf("error stating destination %q: %w", destinationName, err)
					}
				} else {
					if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
						copyFile = true
					}
					if input.CheckTimestamps {
						if !EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), time.Second) {
							copyFile = true
						}
					}
				}
				if copyFile {
					err := Copy(context.Background(), &CopyInput{
						SourceName:            sourceName,
						SourceFileSystem:      input.SourceFileSystem,
						DestinationName:       destinationName,
						DestinationFileSystem: input.DestinationFileSystem,
						Parents:               true,
						Logger:                input.Logger,
					})
					if err != nil {
						return fmt.Errorf("error copying %q to %q: %w", sourceName, destinationName, err)
					}
				} else {
					if input.Logger != nil {
						input.Logger.Log("Skipping file", map[string]interface{}{
							"src": sourceName,
						})
					}
				}
				return nil
			})
		}
		// break if count is greater than or at the limit
		if input.Limit != -1 && count >= input.Limit {
			break
		}
	}

	// wait for all files in directory to copy before returning
	if err := wg.Wait(); err != nil {
		return 0, fmt.Errorf("error synchronizing directory %q to %q: %w", input.SourceDirectory, input.DestinationDirectory, err)
	}

	return count, nil
}
