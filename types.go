package hedwig

import (
	"context"
)

// MessageTypeMajorVersion is a tuple of message typa and major version
type MessageTypeMajorVersion struct {
	// Message type
	MessageType string
	// Message major version
	MajorVersion uint
}

// GetLoggerFunc returns the logger object
type GetLoggerFunc func(ctx context.Context) Logger
