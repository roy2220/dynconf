package storage

import (
	"context"
	"errors"
)

// Storage represents a generic key/value storage.
type Storage interface {
	// GetValue returns the value for the given key. If the value exists, the version of
	// the value is returned with a non-nil value.
	GetValue(ctx context.Context, key string) (value string, version Version, err error)

	// WaitForValue waits for a value for the given key. When the value for the given
	// key does not exist, the function blocks until the value is set. When the value
	// for the given key exists, the value is immediately returned if it has a version
	// later than the given version, otherwise the function blocks until a new value
	// is set. A version given with a nil value is meaningful as it represents an
	// infinitely old version. The latest version of the storage is always returned
	// with a non-nil value.
	WaitForValue(ctx context.Context, key string, version Version) (value string, latestVersion Version, err error)

	// SetValue sets the value for the given key. The latest version of the storage is
	// always returned with a non-nil value.
	SetValue(ctx context.Context, key, value string) (latestVersion Version, err error)

	// DeleteValue deletes the value for the given key. If the value exists, the latest
	// version of the storage is returned with a non-nil value.
	DeleteValue(ctx context.Context, key string) (latestVersion Version, err error)

	// Inspect returns the detailed information of the storage for testing or debugging
	// purposes.
	Inspect(ctx context.Context) (details Details, err error)

	// Close releases resources associated with the storage.
	Close() (err error)
}

// Version represents a identifier to reflect revisions in a storage.
type Version interface{}

// Details represents the detailed information of a storage.
type Details struct {
	IsClosed  bool
	Key2Value map[string]ValueDetails
	Version   Version
}

// ValueDetails represents the detailed information of a value.
type ValueDetails struct {
	V       string
	Version Version
}

// ErrClosed is returned when operating a storage closed.
var ErrClosed = errors.New("storage: closed")
