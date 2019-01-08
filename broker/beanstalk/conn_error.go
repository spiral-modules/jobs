package beanstalk

import "strings"

var connErrors = []string{"pipe", "read tcp", "write tcp", "EOF"}

// connError wraps regular error as pipe error and forces pool to re-init the connection.
type connError struct {
	// Error is underlying error.
	Caused error
}

// Error return error code.
func (e connError) Error() string {
	return e.Caused.Error()
}

// isConnError indicates that error is related to dead socket.
func isConnError(err error) bool {
	for _, errStr := range connErrors {
		// golang...
		if strings.Contains(err.Error(), errStr) {
			return true
		}
	}

	return false
}
