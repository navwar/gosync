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
	"os"
	"time"
)

// Copy copies a file from the input file system to the output file system
func Copy(ctx context.Context, input *CopyInput) error {
	if input.Logger != nil {
		_ = input.Logger.Log("Copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	// stat source file
	var sourceModTime time.Time
	if input.SourceFileInfo != nil {
		// input provides file info for source
		sourceModTime = input.SourceFileInfo.ModTime()
	} else {
		// request file info for soruce from file system
		fi, err := input.SourceFileSystem.Stat(ctx, input.SourceName)
		if err != nil {
			return fmt.Errorf("error stating source file at %q: %w", input.SourceName, err)
		}
		sourceModTime = fi.ModTime()
	}

	// open source file
	sourceFile, err := input.SourceFileSystem.Open(ctx, input.SourceName)
	if err != nil {
		return fmt.Errorf("error opening source file at %q: %w", input.SourceName, err)
	}

	// check parent directory and create it if allowed
	if input.CheckParents {
		parent := input.DestinationFileSystem.Dir(input.DestinationName)
		if _, statError := input.DestinationFileSystem.Stat(ctx, parent); statError != nil {
			if input.DestinationFileSystem.IsNotExist(statError) {
				if !input.MakeParents {
					return fmt.Errorf(
						"parent directory for destination %q does not exist and parents parameter is false",
						input.DestinationName,
					)
				}
				mkdirAllError := input.DestinationFileSystem.MkdirAll(ctx, parent, 0755)
				if mkdirAllError != nil {
					return fmt.Errorf("error creating parent directories for %q", input.DestinationName)
				}
			} else {
				return fmt.Errorf("error stating destination parent %q: %w", parent, statError)
			}
		}
	}

	// open destination file
	destinationFile, err := input.DestinationFileSystem.OpenFile(ctx, input.DestinationName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		_ = sourceFile.Close() // silently close source file
		return fmt.Errorf("error creating destination file at %q: %w", input.SourceName, err)
	}

	// copy bytes from source to destination
	written, err := sourceFile.WriteTo(ctx, destinationFile)
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
	err = input.DestinationFileSystem.Chtimes(ctx, input.DestinationName, time.Now(), sourceModTime)
	if err != nil {
		return fmt.Errorf(
			"error changing timestamps for destination %q after copying: %w",
			input.DestinationName,
			err)
	}

	if input.Logger != nil {
		_ = input.Logger.Log("Done copying file", map[string]interface{}{
			"src":     input.SourceName,
			"dst":     input.DestinationName,
			"written": written,
		})
	}

	return nil
}
