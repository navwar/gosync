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
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

func SyncDirectory(ctx context.Context, sourceDirectory string, sourceFileSystem FileSystem, destinationDirectory string, destinationFileSystem FileSystem, checkTimestamps bool, limit int) (int, error) {
	fmt.Fprintln(os.Stderr, fmt.Sprintf("SyncDirectory(%q,%q,%v,%d)", sourceDirectory, destinationDirectory, checkTimestamps, limit))

	// limit is zero
	if limit == 0 {
		return 0, nil
	}

	sourceDirectoryEntries, err := sourceFileSystem.ReadDir(ctx, sourceDirectory)
	if err != nil {
		return 0, fmt.Errorf("error reading source directory %q: %w", sourceDirectory, err)
	}

	fmt.Fprintln(os.Stderr, fmt.Sprintf("sourceFileSystem.ReadDir(ctx,%q) => %s", sourceDirectory, sourceDirectoryEntries))

	// wait group
	var wg errgroup.Group

	// declare count
	count := 0

	for _, sourceDirectoryEntry := range sourceDirectoryEntries {
		sourceDirectoryEntry := sourceDirectoryEntry
		sourceName := filepath.Join(sourceDirectory, sourceDirectoryEntry.Name())
		destinationName := filepath.Join(destinationDirectory, sourceDirectoryEntry.Name())
		if sourceDirectoryEntry.IsDir() {
			// synchronize directory and wait until all files are finished copying
			directoryLimit := -1
			if limit != -1 {
				directoryLimit = limit - count
			}
			c, err := SyncDirectory(
				ctx,
				sourceName,
				sourceFileSystem,
				destinationName,
				destinationFileSystem,
				checkTimestamps,
				directoryLimit)
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			count += 1
			wg.Go(func() error {
				copyFile := false
				destinationFileInfo, err := destinationFileSystem.Stat(ctx, destinationName)
				if err != nil {
					if destinationFileSystem.IsNotExist(err) {
						copyFile = true
					} else {
						return fmt.Errorf("error stating destination %q: %w", destinationName, err)
					}
				} else {
					if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
						copyFile = true
					}
					if checkTimestamps {
						if sourceDirectoryEntry.ModTime() != destinationFileInfo.ModTime() {
							copyFile = true
						}
					}
				}
				if copyFile {
					err := Copy(context.Background(), sourceName, sourceFileSystem, destinationName, destinationFileSystem, true)
					if err != nil {
						return fmt.Errorf("error copying %q to %q: %w", sourceName, destinationName, err)
					}
				}
				return nil
			})
		}
		// break if count is greater than or at the limit
		if limit != -1 && count >= limit {
			break
		}
	}

	// wait for all files in directory to copy before returning
	if err := wg.Wait(); err != nil {
		return 0, fmt.Errorf("error synchronizing directory %q to %q: %w", sourceDirectory, destinationDirectory, err)
	}

	return count, nil
}
