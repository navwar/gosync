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
	"io"
	"os"
)

// Split splits the path using the path separator for the local operating system
func Copy(ctx context.Context, input *CopyInput) error {
	if input.Logger != nil {
		input.Logger.Log("Copying file", map[string]interface{}{
			"src": input.SourceName,
			"dst": input.DestinationName,
		})
	}

	// open source file
	sourceFile, err := input.SourceFileSystem.Open(ctx, input.SourceName)
	if err != nil {
		return fmt.Errorf("error opening source file at %q: %w", input.SourceName, err)
	}

	// check parent directory and create it if allowed
	parent := input.DestinationFileSystem.Dir(input.DestinationName)
	if _, err := input.DestinationFileSystem.Stat(ctx, parent); err != nil {
		if input.DestinationFileSystem.IsNotExist(err) {
			if !input.Parents {
				return fmt.Errorf(
					"parent directory for destination %q does not exist and parents parameter is false",
					input.DestinationName,
				)
			}
			err := input.DestinationFileSystem.MkdirAll(ctx, parent, 0755)
			if err != nil {
				return fmt.Errorf("error creating parent directories for %q", input.DestinationName)
			}
		} else {
			return fmt.Errorf("error stating destination parent %q: %w", parent, err)
		}
	}

	// open destination file
	destinationFile, err := input.DestinationFileSystem.OpenFile(ctx, input.DestinationName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		_ = sourceFile.Close() // silently close source file
		return fmt.Errorf("error creating destination file at %q: %w", input.SourceName, err)
	}

	// copy bytes from source to destination
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

	if input.Logger != nil {
		input.Logger.Log("Done copying file", map[string]interface{}{
			"src":     input.SourceName,
			"dst":     input.DestinationName,
			"written": written,
		})
	}

	return nil
}
