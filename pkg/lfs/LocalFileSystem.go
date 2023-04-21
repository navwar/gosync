// =================================================================
//
// Work of the U.S. Department of Defense, Defense Digital Service.
// Released as open source under the MIT License.  See LICENSE file.
//
// =================================================================

package lfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/afero"

	"github.com/deptofdefense/gosync/pkg/fs"

	"golang.org/x/sync/errgroup"
)

type LocalFileSystem struct {
	fs   afero.Fs
	iofs afero.IOFS
	root string
}

func (lfs *LocalFileSystem) Copy(ctx context.Context, input *fs.CopyInput) error {
	if input.Logger != nil {
		input.Logger.Log("Copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	parent := filepath.Dir(input.DestinationName)
	if _, err := lfs.fs.Stat(parent); err != nil {
		if lfs.IsNotExist(err) {
			if !input.Parents {
				return fmt.Errorf(
					"parent directory for destination %q does not exist and parents parameter is false",
					input.DestinationName,
				)
			}
			err := lfs.fs.MkdirAll(parent, 0755)
			if err != nil {
				return fmt.Errorf("error creating parent directories for %q", input.DestinationName)
			}
		} else {
			return fmt.Errorf("error stating destination parent %q: %w", parent, err)
		}
	}

	sourceFileInfo, err := lfs.fs.Stat(input.SourceName)
	if err != nil {
		return fmt.Errorf("error stating source file at %q: %w", input.SourceName, err)
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
	err = lfs.fs.Chtimes(input.DestinationName, time.Now(), sourceFileInfo.ModTime())
	if err != nil {
		return fmt.Errorf("error changing timestamps for destination %q after copying: %w", input.DestinationName, err)
	}

	if input.Logger != nil {
		input.Logger.Log("Done copying file", map[string]interface{}{
			"src":     input.SourceName,
			"dst":     input.DestinationName,
			"written": written,
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

func (lfs *LocalFileSystem) MkdirAll(ctx context.Context, name string, mode os.FileMode) error {
	return lfs.fs.MkdirAll(name, mode)
}

func (lfs *LocalFileSystem) Open(ctx context.Context, name string) (fs.File, error) {
	f, err := lfs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (lfs *LocalFileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (fs.File, error) {
	f, err := lfs.fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (lfs *LocalFileSystem) ReadDir(ctx context.Context, name string) ([]fs.DirectoryEntry, error) {
	directoryEntries := []fs.DirectoryEntry{}
	readDirOutput, err := lfs.iofs.ReadDir(name)
	if err != nil {
		return nil, err
	}
	for _, directoryEntry := range readDirOutput {
		directoryEntries = append(directoryEntries, &LocalDirectoryEntry{
			de: directoryEntry,
		})
	}
	return directoryEntries, nil
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
	sourceDirectoryEntries, err := lfs.ReadDir(ctx, input.SourceDirectory)
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
				SourceDirectory:      sourceName,
				DestinationDirectory: destinationName,
				CheckTimestamps:      input.CheckTimestamps,
				Limit:                directoryLimit,
				Logger:               input.Logger,
				MaxThreads:           input.MaxThreads,
			})
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			count += 1
			wg.Go(func() error {
				copyFile := false
				destinationFileInfo, err := lfs.Stat(ctx, destinationName)
				if err != nil {
					if lfs.IsNotExist(err) {
						copyFile = true
					} else {
						return fmt.Errorf("error stating destination %q: %w", destinationName, err)
					}
				} else {
					if sourceDirectoryEntry.Size() != destinationFileInfo.Size() {
						copyFile = true
					}
					if input.CheckTimestamps {
						if sourceDirectoryEntry.ModTime() != destinationFileInfo.ModTime() {
							copyFile = true
						}
					}
				}
				if copyFile {
					err := lfs.Copy(context.Background(), &fs.CopyInput{
						SourceName:      sourceName,
						DestinationName: destinationName,
						Parents:         true,
						Logger:          input.Logger,
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

// func (lfs *LocalFileSystem) Sync(ctx context.Context, source string, destination string, parents bool, checkTimestamps bool, limit int, logger fs.Logger) (int, error) {
func (lfs *LocalFileSystem) Sync(ctx context.Context, input *fs.SyncInput) (int, error) {

	sourceFileInfo, err := lfs.Stat(ctx, input.Source)
	if err != nil {
		if lfs.IsNotExist(err) {
			return 0, fmt.Errorf("source does not exist %q: %w", input.Source, err)
		}
	}

	// if source is a directory
	if sourceFileInfo.IsDir() {
		if _, err := lfs.Stat(ctx, input.Destination); err != nil {
			if lfs.IsNotExist(err) {
				if !input.Parents {
					return 0, fmt.Errorf("destination directory does not exist and parents is false: %q", input.Destination)
				}
			}
		}
		count, err := lfs.SyncDirectory(ctx, &fs.SyncDirectoryInput{
			SourceDirectory:      input.Source,
			DestinationDirectory: input.Destination,
			CheckTimestamps:      input.CheckTimestamps,
			Limit:                input.Limit,
			Logger:               input.Logger,
			MaxThreads:           input.MaxThreads,
		})
		if err != nil {
			return 0, fmt.Errorf("error syncing source directory %q to destination directory %q: %w", input.Source, input.Destination, err)
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
			if sourceFileInfo.ModTime() != destinationFileInfo.ModTime() {
				copyFile = true
			}
		}
	}

	if copyFile {
		err = lfs.Copy(ctx, &fs.CopyInput{
			SourceName:      input.Source,
			DestinationName: input.Destination,
			Parents:         input.Parents,
			Logger:          input.Logger,
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
