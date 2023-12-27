// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/navwar/gosync/pkg/fs"

	"golang.org/x/sync/errgroup"
)

type LocalFileSystem struct {
	fs   afero.Fs
	iofs afero.IOFS
	root string
}

func (lfs *LocalFileSystem) Chtimes(ctx context.Context, name string, atime time.Time, mtime time.Time) error {
	err := lfs.fs.Chtimes(name, atime, mtime)
	if err != nil {
		return fmt.Errorf("error changing timestamps for file %q: %w", name, err)
	}
	return nil
}

func (lfs *LocalFileSystem) Copy(ctx context.Context, input *fs.CopyInput) error {
	if input.Logger != nil {
		_ = input.Logger.Log("Copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	if input.CheckParents {
		parent := filepath.Dir(input.DestinationName)
		if _, statError := lfs.fs.Stat(parent); statError != nil {
			if lfs.IsNotExist(statError) {
				if !input.MakeParents {
					return fmt.Errorf(
						"parent directory for destination %q does not exist and parents parameter is false",
						input.DestinationName,
					)
				}
				mkdirAllError := lfs.fs.MkdirAll(parent, 0755)
				if mkdirAllError != nil {
					return fmt.Errorf("error creating parent directories for %q", input.DestinationName)
				}
			} else {
				return fmt.Errorf("error stating destination parent %q: %w", parent, statError)
			}
		}
	}

	var sourceModTime time.Time
	if input.SourceFileInfo != nil {
		// input provides file info for source
		sourceModTime = input.SourceFileInfo.ModTime()
	} else {
		fi, err := lfs.fs.Stat(input.SourceName)
		if err != nil {
			return fmt.Errorf("error stating source file at %q: %w", input.SourceName, err)
		}
		sourceModTime = fi.ModTime()
	}

	sourceFile, err := lfs.fs.Open(input.SourceName)
	if err != nil {
		return fmt.Errorf("error opening source file at %q: %w", input.SourceName, err)
	}

	destinationFile, err := lfs.fs.OpenFile(input.DestinationName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		_ = sourceFile.Close() // silently close source file
		return fmt.Errorf("error creating destination file at %q: %w", input.DestinationName, err)
	}

	written, err := io.Copy(destinationFile, sourceFile)
	if err != nil {
		_ = sourceFile.Close()      // silently close source file
		_ = destinationFile.Close() // silently close destination file
		return fmt.Errorf("error copying from %q to %q: %w", input.SourceName, input.DestinationName, err)
	}

	err = sourceFile.Close()
	if err != nil {
		_ = destinationFile.Close() // silently close destination file
		return fmt.Errorf("error closing source file after copying: %w", err)
	}

	err = destinationFile.Close()
	if err != nil {
		return fmt.Errorf("error closing destination file after copying: %w", err)
	}

	// Preserve Modification time
	err = lfs.Chtimes(ctx, input.DestinationName, time.Now(), sourceModTime)
	if err != nil {
		return fmt.Errorf("error changing timestamps for destination %q after copying: %w", input.DestinationName, err)
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Done copying file", map[string]interface{}{
			"src":  input.SourceName,
			"dst":  input.DestinationName,
			"size": written,
		})
	}

	return nil
}

func (lfs *LocalFileSystem) Dir(name string) string {
	return filepath.Dir(name)
}

func (lfs *LocalFileSystem) IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func (lfs *LocalFileSystem) Join(name ...string) string {
	return filepath.Join(name...)
}

func (lfs *LocalFileSystem) MagicNumber(ctx context.Context, name string) ([]byte, error) {
	data := []byte{0, 0}
	_, err := lfs.ReadFile(ctx, name, data)
	if err != nil {
		return nil, fmt.Errorf("error retrieving magic number from file %q: %w", name, err)
	}
	return data, nil
}

// MagicNumbers retrieves the magic numbers for the files with the names in the array.
// If an element is empty, then skips.
func (lfs *LocalFileSystem) MagicNumbers(ctx context.Context, names []string, threads int) ([][]byte, error) {
	magicNumbers := make([][]byte, len(names))
	for i, name := range names {
		if len(name) > 0 {
			magicNumber, err := lfs.MagicNumber(ctx, name)
			if err != nil {
				return magicNumbers, fmt.Errorf("error retrieving magic number for file at index %d: %w", i, err)
			}
			magicNumbers[i] = magicNumber
		}
	}
	return magicNumbers, nil
}

func (lfs *LocalFileSystem) MkdirAll(ctx context.Context, name string, mode os.FileMode) error {
	return lfs.fs.MkdirAll(name, mode)
}

func (lfs *LocalFileSystem) MustRelative(ctx context.Context, base string, target string) string {
	relpath, err := lfs.Relative(ctx, base, target)
	if err != nil {
		panic(err)
	}
	return relpath
}

func (lfs *LocalFileSystem) Open(ctx context.Context, name string) (fs.File, error) {
	f, err := lfs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return NewLocalFile(f), nil
}

func (lfs *LocalFileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (fs.File, error) {
	f, err := lfs.fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return NewLocalFile(f), nil
}

func (lfs *LocalFileSystem) ReadDir(ctx context.Context, name string, recursive bool) ([]fs.DirectoryEntryInterface, error) {
	directoryEntries := []fs.DirectoryEntryInterface{}
	readDirOutput, err := lfs.iofs.ReadDir(name)
	if err != nil {
		return nil, err
	}
	for _, directoryEntry := range readDirOutput {
		directoryEntries = append(directoryEntries, &LocalDirectoryEntry{
			de: directoryEntry,
		})
	}
	if recursive {
		recursiveDirectoryEntries := []fs.DirectoryEntryInterface{}
		for _, directoryEntry := range directoryEntries {
			if directoryEntry.IsDir() {
				moreDirectoryEntries, err := lfs.ReadDir(ctx, lfs.Join(name, directoryEntry.Name()), recursive)
				if err != nil {
					return nil, err
				}
				for _, de := range moreDirectoryEntries {
					recursiveDirectoryEntries = append(recursiveDirectoryEntries, fs.NewDirectoryEntry(
						lfs.Join(directoryEntry.Name(), de.Name()),
						de.IsDir(),
						de.ModTime(),
						de.Size()),
					)
				}
			}
		}
		directoryEntries = append(directoryEntries, recursiveDirectoryEntries...)
	}
	return directoryEntries, nil
}

func (lfs *LocalFileSystem) ReadFile(ctx context.Context, name string, data []byte) (int, error) {
	file, err := lfs.Open(ctx, name)
	if err != nil {
		return 0, err
	}
	n, err := file.Read(data)
	if err != nil {
		return n, err
	}
	err = file.Close()
	if err != nil {
		return n, err
	}
	return n, nil
}

func (lfs *LocalFileSystem) Relative(ctx context.Context, basepath string, targetpath string) (string, error) {
	return filepath.Rel(basepath, targetpath)
}

func (lfs *LocalFileSystem) RemoveFile(ctx context.Context, name string) error {
	err := lfs.fs.Remove(name)
	if err != nil {
		return fmt.Errorf("error removing path %q: %w", name, err)
	}
	return nil
}

func (lfs *LocalFileSystem) RemoveFiles(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	for _, name := range names {
		err := lfs.fs.Remove(name)
		if err != nil {
			return fmt.Errorf("error removing path %q: %w", name, err)
		}
	}
	return nil
}

func (lfs *LocalFileSystem) RemoveDirectory(ctx context.Context, name string, recursive bool) error {
	if recursive {
		err := lfs.fs.RemoveAll(name)
		if err != nil {
			return fmt.Errorf("error removing path %q: %w", name, err)
		}
		return nil
	}
	err := lfs.fs.Remove(name)
	if err != nil {
		return fmt.Errorf("error removing path %q: %w", name, err)
	}
	return nil
}

func (lfs *LocalFileSystem) RemoveDirectories(ctx context.Context, names []string, recursive bool) error {
	if len(names) == 0 {
		return nil
	}
	if recursive {
		for _, name := range names {
			err := lfs.fs.RemoveAll(name)
			if err != nil {
				return fmt.Errorf("error removing path %q: %w", name, err)
			}
		}
		return nil
	}
	for _, name := range names {
		err := lfs.fs.Remove(name)
		if err != nil {
			return fmt.Errorf("error removing path %q: %w", name, err)
		}
	}
	return nil
}

func (lfs *LocalFileSystem) Root() string {
	return lfs.root
}

func (lfs *LocalFileSystem) Size(ctx context.Context, name string) (int64, error) {
	fi, err := lfs.fs.Stat(name)
	if err != nil {
		return int64(0), err
	}
	return fi.Size(), nil
}

func (lfs *LocalFileSystem) Stat(ctx context.Context, name string) (fs.FileInfo, error) {
	fi, err := lfs.fs.Stat(name)
	if err != nil {
		return nil, err
	}
	return NewLocalFileInfo(fi.Name(), fi.ModTime(), fi.IsDir(), fi.Size()), nil
}

func (lfs *LocalFileSystem) SyncDirectory(ctx context.Context, input *fs.SyncDirectoryInput) (int, error) {
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
	sourceDirectoryEntriesByName := map[string]fs.DirectoryEntryInterface{}
	sourceDirectoryEntries, err := lfs.ReadDir(ctx, input.SourceDirectory, false)
	if err != nil {
		return 0, fmt.Errorf("error reading source directory %q: %w", input.SourceDirectory, err)
	} else {
		for _, sourceDirectoryEntry := range sourceDirectoryEntries {
			sourceName := filepath.Join(input.SourceDirectory, sourceDirectoryEntry.Name())
			sourceDirectoryEntriesByName[sourceName] = sourceDirectoryEntry
		}
	}

	// list destination directory to cache file info
	destinationDirectoryEntriesByName := map[string]fs.DirectoryEntryInterface{}
	destinationDirectoryEntries, err := lfs.ReadDir(ctx, input.DestinationDirectory, false)
	if err != nil {
		if !lfs.IsNotExist(err) {
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
			"delete":  input.Delete,
			"dst":     input.DestinationDirectory,
			"exclude": input.Exclude,
			"files":   len(sourceDirectoryEntries),
			"src":     input.SourceDirectory,
			"threads": input.MaxThreads,
		})
	}

	// create directory if not exist
	if _, statError := lfs.Stat(ctx, input.DestinationDirectory); statError != nil {
		if lfs.IsNotExist(statError) {
			mkdirAllError := lfs.MkdirAll(ctx, input.DestinationDirectory, 0755)
			if mkdirAllError != nil {
				return 0, fmt.Errorf("error creating destination directory for %q", input.DestinationDirectory)
			}
		} else {
			return 0, fmt.Errorf("error stating destination directory %q: %w", input.DestinationDirectory, statError)
		}
	}

	// delete files and directories at destination that do not exist at source
	if input.Delete {
		filesToBeDeleted := []string{}
		directoriesToBeDeleted := []string{}
		for destinationDirectoryName, destinationDirectoryEntry := range destinationDirectoryEntriesByName {
			destinationSuffix := destinationDirectoryName[len(input.DestinationDirectory):]
			sourcePath := lfs.Join(input.SourceDirectory, destinationSuffix)
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
		err := lfs.RemoveDirectories(ctx, directoriesToBeDeleted, true)
		if err != nil {
			return 0, fmt.Errorf("error removing %d directories at destination that do not exist at source: %w", len(directoriesToBeDeleted), err)
		}
		_ = input.Logger.Log("Deleting files", map[string]interface{}{
			"files": filesToBeDeleted,
		})
		err = lfs.RemoveFiles(ctx, filesToBeDeleted)
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

	for _, sourceDirectoryEntry := range sourceDirectoryEntries {
		sourceDirectoryEntry := sourceDirectoryEntry
		sourceName := filepath.Join(input.SourceDirectory, sourceDirectoryEntry.Name())
		destinationName := filepath.Join(input.DestinationDirectory, sourceDirectoryEntry.Name())
		if sourceDirectoryEntry.IsDir() {
			// synchronize directory and wait until all files are finished copying
			directoryLimit := -1
			if input.Limit != -1 {
				directoryLimit = input.Limit - count
			}
			c, err := lfs.SyncDirectory(ctx, &fs.SyncDirectoryInput{
				CheckTimestamps:      input.CheckTimestamps,
				DestinationDirectory: destinationName,
				Exclude:              input.Exclude,
				Limit:                directoryLimit,
				Logger:               input.Logger,
				MaxThreads:           input.MaxThreads,
				SourceDirectory:      sourceName,
				TimestampPrecision:   input.TimestampPrecision,
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
					var destinationFileInfo fs.FileInfo
					if dfi, ok := destinationDirectoryEntriesByName[destinationName]; ok {
						// use cached destination file info
						destinationFileInfo = dfi
						// compare sizes
						if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
							copyFile = true
						}
						// compare timestamps
						if input.CheckTimestamps {
							if !fs.EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
								copyFile = true
							}
						}
					} else {
						// destination not cached
						dfi, err := lfs.Stat(ctx, destinationName)
						if err != nil {
							if lfs.IsNotExist(err) {
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
								if !fs.EqualTimestamp(sourceDirectoryEntry.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
									copyFile = true
								}
							}
						}
					}
					if copyFile {
						err := lfs.Copy(context.Background(), &fs.CopyInput{
							CheckParents:    false, // parent directory already created
							SourceName:      sourceName,
							SourceFileInfo:  sourceDirectoryEntry,
							DestinationName: destinationName,
							Logger:          input.Logger,
							MakeParents:     false, // parent directory already created
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

// Sync synchronizes the input directory to the output directory in the local file system
func (lfs *LocalFileSystem) Sync(ctx context.Context, input *fs.SyncInput) (int, error) {

	if input.Logger != nil {
		_ = input.Logger.Log("Synchronizing", map[string]interface{}{
			"delete":  input.Delete,
			"dst":     input.Destination,
			"exclude": input.Exclude,
			"src":     input.Source,
			"threads": input.MaxThreads,
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

	sourceFileInfo, err := lfs.Stat(ctx, input.Source)
	if err != nil {
		if lfs.IsNotExist(err) {
			return 0, fmt.Errorf("source does not exist %q: %w", input.Source, err)
		}
		return 0, fmt.Errorf("error stating source %q: %w", input.Source, err)
	}

	// if source is a directory
	if sourceFileInfo.IsDir() {
		if _, statError := lfs.Stat(ctx, input.Destination); statError != nil {
			if lfs.IsNotExist(statError) {
				if !input.Parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", input.Destination)
				}
			}
		}
		count, syncDirectoryError := lfs.SyncDirectory(ctx, &fs.SyncDirectoryInput{
			CheckTimestamps:      input.CheckTimestamps,
			Delete:               input.Delete,
			DestinationDirectory: input.Destination,
			Exclude:              input.Exclude,
			Limit:                input.Limit,
			Logger:               input.Logger,
			MaxThreads:           input.MaxThreads,
			SourceDirectory:      input.Source,
			TimestampPrecision:   input.TimestampPrecision,
		})
		if syncDirectoryError != nil {
			return 0, fmt.Errorf("error syncing source directory %q to destination directory %q: %w", input.Source, input.Destination, syncDirectoryError)
		}
		return count, nil
	}

	// if source is a file
	copyFile := false

	destinationFileInfo, err := lfs.Stat(ctx, input.Destination)
	if err != nil {
		if lfs.IsNotExist(err) {
			copyFile = true
		} else {
			return 0, fmt.Errorf("error stating destination %q: %w", input.Destination, err)
		}
	} else {
		if sourceFileInfo.Size() != destinationFileInfo.Size() {
			copyFile = true
		}
		if input.CheckTimestamps {
			if !fs.EqualTimestamp(sourceFileInfo.ModTime(), destinationFileInfo.ModTime(), input.TimestampPrecision) {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = lfs.Copy(ctx, &fs.CopyInput{
			CheckParents:    true, // check if parent for destination exists
			SourceName:      input.Source,
			DestinationName: input.Destination,
			Logger:          input.Logger,
			MakeParents:     input.Parents, // create destination parents if not exist
		})
		if err != nil {
			return 0, fmt.Errorf("error copying %q to %q: %w", input.Source, input.Destination, err)
		}
		return 1, nil
	}

	return 0, nil
}

func NewLocalFileSystem(rootPath string) *LocalFileSystem {
	lfs := afero.NewBasePathFs(afero.NewOsFs(), rootPath)
	return &LocalFileSystem{
		fs:   lfs,
		iofs: afero.NewIOFS(lfs),
		root: rootPath,
	}
}

func NewReadOnlyLocalSystem(rootPath string) *LocalFileSystem {
	lfs := afero.NewBasePathFs(afero.NewReadOnlyFs(afero.NewOsFs()), rootPath)
	return &LocalFileSystem{
		fs:   lfs,
		iofs: afero.NewIOFS(lfs),
		root: rootPath,
	}
}
