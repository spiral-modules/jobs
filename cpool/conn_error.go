package cpool

import "strings"

var connErrors = []string{"pipe", "read tcp", "write tcp", "EOF"}

// ConnError wraps regular error as pipe error and forces pool to re-init the connection.
type ConnError struct {
	// Error is underlying error.
	Err error
}

// Error return error code.
func (e ConnError) Error() string {
	return e.Err.Error()
}

// IsConnError indicates that error is related to dead socket.
func IsConnError(err error) bool {
	for _, errStr := range connErrors {
		// golang...
		if strings.Contains(err.Error(), errStr) {
			return true
		}
	}

	return false
}
