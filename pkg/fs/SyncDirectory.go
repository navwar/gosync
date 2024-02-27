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
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"
)

func SyncDirectory(ctx context.Context, input *SyncDirectoryInput) (int, error) {

	// limit is zero
	if input.Limit == 0 {
		return 0, nil
	}

	skip := false
	if len(input.Exclude) > 0 {
		for _, exclude := range input.Exclude {
			if strings.HasPrefix(exclude, "*") {
				if strings.HasSuffix(exclude, "*") {
					if strings.Contains(input.SourceDirectory, exclude[1:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if strings.HasSuffix(input.SourceDirectory, exclude[1:]) {
						skip = true
						break
					}
				}
			} else {
				if strings.HasSuffix(exclude, "*") {
					if strings.HasPrefix(input.SourceDirectory, exclude[0:len(exclude)-1]) {
						skip = true
						break
					}
				} else {
					if input.SourceDirectory == exclude {
						skip = true
						break
					}
				}
			}
		}
	}

	if skip {
		if input.Logger != nil {
			_ = input.Logger.Log("Skipping directory", map[string]interface{}{
				"src": input.SourceDirectory,
			})
		}
		return 0, nil
	}

	// list source directory
	sourceDirectoryEntriesByName := map[string]DirectoryEntryInterface{}
	sourceDirectoryEntries, err := input.SourceFileSystem.ReadDir(ctx, input.SourceDirectory, false)
	if err != nil {
		return 0, fmt.Errorf("error reading source directory %q: %w", input.SourceDirectory, err)
	} else {
		for _, sourceDirectoryEntry := range sourceDirectoryEntries {
			sourceName := filepath.Join(input.SourceDirectory, sourceDirectoryEntry.Name())
			sourceDirectoryEntriesByName[sourceName] = sourceDirectoryEntry
		}
	}

	// list destination directory to cache file info
	destinationDirectoryEntriesByName := map[string]DirectoryEntryInterface{}
	destinationDirectoryEntries, err := input.DestinationFileSystem.ReadDir(ctx, input.DestinationDirectory, false)
	if err != nil {
		if !input.DestinationFileSystem.IsNotExist(err) {
			return 0, fmt.Errorf("error reading destination directory %q: %w", input.SourceDirectory, err)
		}
	} else {
		for _, destinationDirectoryEntry := range destinationDirectoryEntries {
			destinationName := filepath.Join(input.DestinationDirectory, destinationDirectoryEntry.Name())
			destinationDirectoryEntriesByName[destinationName] = destinationDirectoryEntry
		}
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Synchronizing Directory", map[string]interface{}{
			"delete":               input.Delete,
			"dst":                  input.DestinationDirectory,
			"exclude":              input.Exclude,
			"files_at_destination": len(destinationDirectoryEntries),
			"files_at_source":      len(sourceDirectoryEntries),
			"src":                  input.SourceDirectory,
			"threads":              input.MaxThreads,
		})
	}

	// create directory if not exist
	if _, statError := input.DestinationFileSystem.Stat(ctx, input.DestinationDirectory); statError != nil {
		if input.DestinationFileSystem.IsNotExist(statError) {
			mkdirAllError := input.DestinationFileSystem.MkdirAll(ctx, input.DestinationDirectory, 0755)
			if mkdirAllError != nil {
				return 0, fmt.Errorf("error creating destination directory for %q: %w", input.DestinationDirectory, mkdirAllError)
			}
		} else {
			return 0, fmt.Errorf("error stating destination directory %q: %w", input.DestinationDirectory, statError)
		}
	}

	// delete files at destination that do not exist at source
	if input.Delete {
		filesToBeDeleted := []string{}
		directoriesToBeDeleted := []string{}
		for destinationDirectoryName, destinationDirectoryEntry := range destinationDirectoryEntriesByName {
			destinationSuffix := destinationDirectoryName[len(input.DestinationDirectory):]
			sourcePath := input.DestinationFileSystem.Join(input.SourceDirectory, destinationSuffix)
			if _, ok := sourceDirectoryEntriesByName[sourcePath]; !ok {
				if destinationDirectoryEntry.IsDir() {
					directoriesToBeDeleted = append(directoriesToBeDeleted, destinationDirectoryName)
				} else {
					filesToBeDeleted = append(filesToBeDeleted, destinationDirectoryName)
				}
			}
		}
		_ = input.Logger.Log("Deleting directories", map[string]interface{}{
			"directories": directoriesToBeDeleted,
		})
		err := input.DestinationFileSystem.RemoveDirectories(ctx, directoriesToBeDeleted, true)
		if err != nil {
			return 0, fmt.Errorf("error removing %d directories at destination that do not exist at source: %w", len(directoriesToBeDeleted), err)
		}
		_ = input.Logger.Log("Deleting files", map[string]interface{}{
			"files": filesToBeDeleted,
		})
		err = input.DestinationFileSystem.RemoveFiles(ctx, filesToBeDeleted)
		if err != nil {
			return 0, fmt.Errorf("error removing %d files at destination that do not exist at source: %w", len(filesToBeDeleted), err)
		}
	}

	// wait group
	var wg errgroup.Group

	// If input.MaxThreads is greater than zero, than set limit.
	if input.MaxThreads > 0 {
		wg.SetLimit(input.MaxThreads)
	}

	// declare count
	count := 0

	// sync source directories to destination
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
				CheckTimestamps:       input.CheckTimestamps,
				DestinationDirectory:  destinationName,
				DestinationFileSystem: input.DestinationFileSystem,
				Exclude:               input.Exclude,
				Limit:                 directoryLimit,
				Logger:                input.Logger,
				MaxThreads:            input.MaxThreads,
				SourceDirectory:       sourceName,
				SourceFileSystem:      input.SourceFileSystem,
				TimestampPrecision:    input.TimestampPrecision,
			})
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			// check if file should be skipped
			skip := false
			if len(input.Exclude) > 0 {
				for _, exclude := range input.Exclude {
					if strings.HasPrefix(exclude, "*") {
						if strings.HasSuffix(exclude, "*") {
							if strings.Contains(sourceName, exclude[1:len(exclude)-1]) {
								skip = true
								break
							}
						} else {
							if strings.HasSuffix(sourceName, exclude[1:]) {
								skip = true
								break
							}
						}
					} else {
						if strings.HasSuffix(exclude, "*") {
							if strings.HasPrefix(sourceName, exclude[0:len(exclude)-1]) {
								skip = true
								break
							}
						} else {
							if sourceName == exclude {
								skip = true
								break
							}
						}
					}
				}
			}
			if skip {
				if input.Logger != nil {
					_ = input.Logger.Log("Skipping file", map[string]interface{}{
						"src": sourceName,
					})
				}
			} else {
				count += 1
				wg.Go(func() error {
					copyFile := false
					// declare destination file info
					var destinationFileInfo FileInfo
					if dfi, ok := destinationDirectoryEntriesByName[destinationName]; ok {
						// use cached destination file info
						destinationFileInfo = dfi
						// compare sizes
						if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
							copyFile = true
						}
						// compare timestamps
						if input.CheckTimestamps {
							if !EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
								copyFile = true
							}
						}
					} else {
						// destination not cached
						dfi, err := input.DestinationFileSystem.Stat(ctx, destinationName)
						if err != nil {
							if input.DestinationFileSystem.IsNotExist(err) {
								copyFile = true
							} else {
								return fmt.Errorf("error stating destination %q: %w", destinationName, err)
							}
						} else {
							// set destination file info
							destinationFileInfo = dfi
							// compare sizes
							if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
								copyFile = true
							}
							// compare timestamps
							if input.CheckTimestamps {
								if !EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
									copyFile = true
								}
							}
						}
					}

					if copyFile {
						err := Copy(context.Background(), &CopyInput{
							CheckParents:          false, // parent directory already created
							SourceName:            sourceName,
							SourceFileInfo:        sourceDirectoryEntry,
							SourceFileSystem:      input.SourceFileSystem,
							DestinationName:       destinationName,
							DestinationFileInfo:   destinationFileInfo,
							DestinationFileSystem: input.DestinationFileSystem,
							Logger:                input.Logger,
							MakeParents:           false, // parent directory already created
						})
						if err != nil {
							return fmt.Errorf("error copying %q to %q: %w", sourceName, destinationName, err)
						}
					} else {
						if input.Logger != nil {
							_ = input.Logger.Log("Skipping file", map[string]interface{}{
								"src": sourceName,
							})
						}
					}
					return nil
				})
			}
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
