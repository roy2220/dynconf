package memorystorage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/roy2220/dynconf/storage"
	. "github.com/roy2220/dynconf/storageprovider/memorystorage/internal"
)

// MemoryStorage implements Storage.
type MemoryStorage struct {
	closure   chan struct{}
	key2Value sync.Map
	version1  uint64
	isClosed1 int32
}

var _ storage.Storage = (*MemoryStorage)(nil)

// Init initialized the memory storage and returns it.
func (ms *MemoryStorage) Init() *MemoryStorage {
	ms.closure = make(chan struct{})
	return ms
}

// GetValue implements Storage.GetValue.
func (ms *MemoryStorage) GetValue(_ context.Context, key string) (string, storage.Version, error) {
	val, version, err := ms.doGetValue(key)
	if err != nil {
		return "", nil, err
	}
	if version == 0 {
		return "", nil, nil
	}
	return val, version, nil
}

func (ms *MemoryStorage) doGetValue(key string) (string, Version, error) {
	if ms.isClosed() {
		return "", 0, storage.ErrClosed
	}
	opaqueValue, ok := ms.key2Value.Load(key)
	if !ok {
		return "", 0, nil
	}
	value := opaqueValue.(*Value)
	val, version := value.Get()
	return val, version, nil
}

// WaitForValue implements Storage.WaitForValue.
func (ms *MemoryStorage) WaitForValue(ctx context.Context, key string, opaqueVersion storage.Version) (string, storage.Version, error) {
	var version Version
	if opaqueVersion == nil {
		version = 0
	} else {
		version = opaqueVersion.(Version)
	}
	val, latestVersion, err := ms.doWaitForValue(ctx, key, version)
	if err != nil {
		return "", nil, err
	}
	return val, latestVersion, nil
}

func (ms *MemoryStorage) doWaitForValue(ctx context.Context, key string, version Version) (string, Version, error) {
	for {
		if ms.isClosed() {
			return "", 0, storage.ErrClosed
		}
		opaqueValue, ok := ms.key2Value.Load(key)
		if !ok {
			opaqueValue, _ = ms.key2Value.LoadOrStore(key, new(Value))
		}
		value := opaqueValue.(*Value)
		waitOptions := WaitOptions{
			Interruption:     ms.closure,
			InterruptedError: storage.ErrClosed,
		}
		val, latestVersion, err := value.WaitFor(ctx, version, waitOptions, func() { ms.key2Value.Delete(key) })
		if err != nil {
			if err == ErrValueIsDeleted {
				continue
			}
			return "", 0, err
		}
		if !ok {
			continue
		}
		return val, latestVersion, nil
	}
}

// SetValue implements Storage.SetValue.
func (ms *MemoryStorage) SetValue(_ context.Context, key, val string) (storage.Version, error) {
	latestVersion, err := ms.doSetValue(key, val)
	if err != nil {
		return nil, err
	}
	return latestVersion, nil
}

func (ms *MemoryStorage) doSetValue(key, val string) (Version, error) {
	for {
		if ms.isClosed() {
			return 0, storage.ErrClosed
		}
		opaqueValue, ok := ms.key2Value.Load(key)
		if !ok {
			opaqueValue, _ = ms.key2Value.LoadOrStore(key, new(Value))
		}
		value := opaqueValue.(*Value)
		latestVersion := ms.getNextVersion()
		if err := value.Set(val, latestVersion); err != nil {
			if err != ErrValueIsDeleted {
				panic("unreachable code")
			}
			continue
		}
		return latestVersion, nil
	}
}

// DeleteValue implements Storage.DeleteValue.
func (ms *MemoryStorage) DeleteValue(_ context.Context, key string) (storage.Version, error) {
	latestVersion, err := ms.doDeleteValue(key)
	if err != nil {
		return nil, err
	}
	if latestVersion == 0 {
		return nil, nil
	}
	return latestVersion, nil
}

func (ms *MemoryStorage) doDeleteValue(key string) (Version, error) {
	if ms.isClosed() {
		return 0, storage.ErrClosed
	}
	opaqueValue, ok := ms.key2Value.Load(key)
	if !ok {
		return 0, nil
	}
	value := opaqueValue.(*Value)
	ok = value.Clear(func() { ms.key2Value.Delete(key) })
	if !ok {
		return 0, nil
	}
	latestVersion := ms.getNextVersion()
	return latestVersion, nil
}

// Inspect implements Storage.Inspect.
func (ms *MemoryStorage) Inspect(_ context.Context) (storage.Details, error) {
	if ms.isClosed() {
		return storage.Details{IsClosed: true}, nil
	}
	var key2Value map[string]storage.ValueDetails
	ms.key2Value.Range(func(opaqueKey, opaqueValue interface{}) bool {
		if key2Value == nil {
			key2Value = make(map[string]storage.ValueDetails)
		}
		key := opaqueKey.(string)
		value := opaqueValue.(*Value)
		val, version := value.Get()
		if version == 0 {
			return true
		}
		key2Value[key] = storage.ValueDetails{
			V:       val,
			Version: version,
		}
		return true
	})
	var opaqueVersion interface{}
	if version := ms.version(); version >= 1 {
		opaqueVersion = version
	}
	return storage.Details{
		Key2Value: key2Value,
		Version:   opaqueVersion,
	}, nil
}

// Close implements Storage.Close.
func (ms *MemoryStorage) Close() error {
	if atomic.SwapInt32(&ms.isClosed1, 1) != 0 {
		return storage.ErrClosed
	}
	close(ms.closure)
	return nil
}

func (ms *MemoryStorage) getNextVersion() Version {
	nextVersion := Version(atomic.AddUint64(&ms.version1, 1))
	return nextVersion
}

func (ms *MemoryStorage) version() Version {
	return Version(atomic.LoadUint64(&ms.version1))
}

func (ms *MemoryStorage) isClosed() bool {
	return atomic.LoadInt32(&ms.isClosed1) != 0
}
