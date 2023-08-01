// ==================================================================================
//
// Work of the U.S. Department of the Navy, Naval Information Warfare Center Pacific.
// Released as open source under the MIT License.  See LICENSE file.
//
// ==================================================================================

package log

import (
	"fmt"
	"io"
	"strings"

	"github.com/aws/smithy-go/logging"
)

type ClientLogger struct {
	logger *SimpleLogger
}

func (c ClientLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	event := fmt.Sprintf(format, v...)
	msg := ""
	details := ""
	if prefix := "Request Signature:\n"; strings.HasPrefix(event, prefix) {
		msg = "Request Signature"
		details = event[len(prefix):]
	} else if prefix := "Request\n"; strings.HasPrefix(event, prefix) {
		msg = "Request"
		details = event[len(prefix):]
	} else if prefix := "Response\n"; strings.HasPrefix(event, prefix) {
		msg = "Response"
		details = event[len(prefix):]
	} else {
		msg = "Client Event"
		details = event
	}
	err := c.logger.Log(msg, map[string]interface{}{
		"details": details,
	})
	if err != nil {
		panic(err)
	}
}

func NewClientLogger(w io.Writer) *ClientLogger {
	return &ClientLogger{logger: NewSimpleLogger(w)}
}
