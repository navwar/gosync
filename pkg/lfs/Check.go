// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package lfs

import (
	"fmt"
)

// Check returns an error if there is a cycle error.
func Check(source string, destination string) error {
	if source == destination {
		return fmt.Errorf("source and destination must be different: %q", "file://"+source)
	}
	sourceDirectories := Split(source)
	destinationDirectories := Split(destination)
	i := 0
	for ; i < len(sourceDirectories) && i < len(destinationDirectories); i++ {
		if sourceDirectories[i] != destinationDirectories[i] {
			return nil
		}
	}
	if len(sourceDirectories)-i > 0 {
		return fmt.Errorf("cycle error: destination %q is a parent of source %q", destination, source)
	} else if len(destinationDirectories)-i > 0 {
		return fmt.Errorf("cycle error: source %q is a parent of destination %q", source, destination)
	}
	return fmt.Errorf("unknown error checking source %q and destination %q", source, destination)
}
