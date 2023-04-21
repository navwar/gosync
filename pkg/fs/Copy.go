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
func Copy(ctx context.Context, sourceName string, sourceFileSystem FileSystem, destinationName string, destinationFileSystem FileSystem, parents bool, logger Logger) error {
	if logger != nil {
		logger.Log("Copying file", map[string]interface{}{
			"src": sourceName,
			"dst": destinationName,
		})
	}

	// open source file
	sourceFile, err := sourceFileSystem.Open(ctx, sourceName)
	if err != nil {
		return fmt.Errorf("error opening source file at %q: %w", sourceName, err)
	}

	// check parent directory and create it if allowed
	parent := destinationFileSystem.Dir(destinationName)
	if _, err := destinationFileSystem.Stat(ctx, parent); err != nil {
		if destinationFileSystem.IsNotExist(err) {
			if !parents {
				return fmt.Errorf(
					"parent directory for destination %q does not exist and parents parameter is false",
					destinationName,
				)
			}
			err := destinationFileSystem.MkdirAll(ctx, parent, 0755)
			if err != nil {
				return fmt.Errorf("error creating parent directories for %q", destinationFileSystem)
			}
		} else {
			return fmt.Errorf("error stating destination parent %q: %w", parent, err)
		}
	}

	// open destination file
	destinationFile, err := destinationFileSystem.OpenFile(ctx, destinationName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		_ = sourceFile.Close() // silently close source file
		return fmt.Errorf("error creating destination file at %q: %w", sourceName, err)
	}

	// copy bytes from source to destination
	written, err := io.Copy(destinationFile, sourceFile)
	if err != nil {
		_ = sourceFile.Close()      // silently close source file
		_ = destinationFile.Close() // silently close destination file
		return fmt.Errorf("error copying from %q to %q: %w", sourceName, destinationName, err)
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

	if logger != nil {
		logger.Log("Done copying file", map[string]interface{}{
			"src":     sourceName,
			"dst":     destinationName,
			"written": written,
		})
	}

	return nil
}
