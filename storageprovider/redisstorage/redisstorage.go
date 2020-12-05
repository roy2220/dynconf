package redisstorage

import (
	"context"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/roy2220/dynconf/storage"
	. "github.com/roy2220/dynconf/storageprovider/redisstorage/internal"
)

type RedisStorage struct {
	client           redis.UniversalClient
	options          options
	getValueScriptID string
	setValueScriptID string
	eventBus         EventBus
}

var _ storage.Storage = (*RedisStorage)(nil)

type Options struct {
	options
	MaxWatchedEventIdleCount int
	MaxWatchedEventIdleTime  time.Duration
}
type options struct {
	Prefix string
}

func (o *options) normalize() {
	if o.Prefix == "" {
		o.Prefix = "{dynconf}:"
	}
}

// Init initialized the redis storage and returns it.
func (rs *RedisStorage) Init(client redis.UniversalClient, options Options) *RedisStorage {
	rs.client = client
	rs.options = options.options
	rs.options.normalize()
	pubSub := client.Subscribe(nil)
	rs.eventBus.Init(pubSub, EventBusOptions{
		ChannelNamePrefix:        options.Prefix,
		MaxWatchedEventIdleCount: options.MaxWatchedEventIdleCount,
		MaxWatchedEventIdleTime:  options.MaxWatchedEventIdleTime,
	})
	return rs
}

// Close implements Storage.Close.
func (rs *RedisStorage) Close() error {
	var errs []error
	if err := rs.eventBus.Close(); err != nil {
		errs = append(errs, convertError(err))
	}
	if err := rs.client.Close(); err != nil {
		errs = append(errs, convertError(err))
	}
	if len(errs) >= 1 {
		return errs[0]
	}
	return nil
}

// GetValue implements Storage.GetValue.
func (rs *RedisStorage) GetValue(ctx context.Context, key string) (string, storage.Version, error) {
	value, version, err := rs.doGetValue(ctx, key, 0)
	if err != nil {
		return "", nil, err
	}
	if version == 0 {
		return "", nil, nil
	}
	return value, version, nil
}

func (rs *RedisStorage) doGetValue(ctx context.Context, key string, version int64) (string, int64, error) {
	const script = `
local latestVersionStr = redis.call("HGET", KEYS[1], ARGV[1])
if latestVersionStr == false then
	return {"", 0}
end
local latestVersion = tonumber(latestVersionStr)
local version = tonumber(ARGV[2])
if latestVersion <= version then
	return {"", -latestVersion}
end
local value = redis.call("HGET", KEYS[2], ARGV[1])
if value == false then
	return {"", 0}
end
return {value, latestVersion}
`
	scriptKeys := []string{
		rs.key2VersionKey(),
		rs.key2ValueKey(),
	}
	scriptArgv := []interface{}{
		key,
		version,
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return "", 0, convertError(err)
	}
	results := result.([]interface{})
	value := results[0].(string)
	latestVersion := results[1].(int64)
	return value, latestVersion, nil
}

// SetValue implements Storage.SetValue.
func (rs *RedisStorage) WaitForValue(ctx context.Context, key string, opaqueVersion storage.Version) (string, storage.Version, error) {
	var version int64
	if opaqueVersion != nil {
		version = opaqueVersion.(int64)
	}
	value, latestVersion, err := rs.doWaitForValue(ctx, key, version)
	if err != nil {
		return "", nil, err
	}
	return value, latestVersion, nil
}

func (rs *RedisStorage) doWaitForValue(ctx context.Context, key string, version int64) (string, int64, error) {
	for {
		value, version, err := func() (string, int64, error) {
			watcher, err := rs.eventBus.AddWatcher(key)
			if err != nil {
				return "", 0, convertError(err)
			}
			defer func() {
				if watcher != nil {
					_ = rs.eventBus.RemoveWatcher(key, watcher)
				}
			}()
			value, latestVersion, err := rs.doGetValue(ctx, key, version)
			if err != nil {
				return "", 0, err
			}
			if latestVersion > version {
				return value, latestVersion, nil
			}
			_, err = watcher.WaitForEvent(ctx)
			if err != nil {
				return "", 0, convertError(err)
			}
			watcher = nil
			return "", 0, nil
		}()
		if err != nil {
			return "", 0, err
		}
		if version >= 1 {
			return value, version, nil
		}
	}
}

// SetValue implements Storage.SetValue.
func (rs *RedisStorage) SetValue(ctx context.Context, key, value string) (storage.Version, error) {
	version, err := rs.doSetValue(ctx, key, value)
	if err != nil {
		return nil, err
	}
	return version, nil
}

func (rs *RedisStorage) doSetValue(ctx context.Context, key, value string) (int64, error) {
	const script = `
local latestVersion = redis.call("INCR", KEYS[1])
local latestVersionStr = tostring(latestVersion)
redis.call("HSET", KEYS[2], ARGV[1], ARGV[2])
redis.call("HSET", KEYS[3], ARGV[1], latestVersionStr)
redis.call("PUBLISH", ARGV[3], latestVersion)
return latestVersion
`
	scriptKeys := []string{
		rs.versionKey(),
		rs.key2ValueKey(),
		rs.key2VersionKey(),
	}
	scriptArgv := []interface{}{
		key,
		value,
		rs.channelName(key),
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return 0, convertError(err)
	}
	latestVersion := result.(int64)
	return latestVersion, nil
}

// DeleteValue implements Storage.DeleteValue.
func (rs *RedisStorage) DeleteValue(ctx context.Context, key string) (storage.Version, error) {
	latestVersion, err := rs.doDeleteValue(ctx, key)
	if err != nil {
		return nil, err
	}
	if latestVersion == 0 {
		return nil, nil
	}
	return latestVersion, nil
}

func (rs *RedisStorage) doDeleteValue(ctx context.Context, key string) (int64, error) {
	const script = `
local n = redis.call("HDEL", KEYS[1], ARGV[1])
if n == 0 then
	return 0
end
local latestVersion = redis.call("INCR", KEYS[2])
redis.call("HDEL", KEYS[3], ARGV[2])
return latestVersion
`
	scriptKeys := []string{
		rs.key2ValueKey(),
		rs.versionKey(),
		rs.key2VersionKey(),
	}
	scriptArgv := []interface{}{
		key,
		key,
	}
	result, err := rs.client.Eval(ctx, script, scriptKeys, scriptArgv...).Result()
	if err != nil {
		return 0, convertError(err)
	}
	latestVersion := result.(int64)
	return latestVersion, nil
}

// Inspect implements Storage.Inspect.
func (rs *RedisStorage) Inspect(ctx context.Context) (storage.Details, error) {
	pipeline := rs.client.TxPipeline()
	cmd1 := pipeline.HGetAll(ctx, rs.key2ValueKey())
	cmd2 := pipeline.HGetAll(ctx, rs.key2VersionKey())
	cmd3 := pipeline.Get(ctx, rs.versionKey())
	pipeline.Exec(ctx)
	result1, err1 := cmd1.Result()
	result2, err2 := cmd2.Result()
	result3, err3 := cmd3.Int64()
	if err3 == redis.Nil {
		result3 = 0
		err3 = nil
	}
	for _, err := range []error{err1, err2, err3} {
		if err != nil {
			if convertError(err) == storage.ErrClosed {
				return storage.Details{IsClosed: true}, nil
			}
			return storage.Details{}, err
		}
	}
	var key2Value map[string]storage.ValueDetails
	for key, value := range result1 {
		if key2Value == nil {
			key2Value = make(map[string]storage.ValueDetails)
		}
		valueDetails := storage.ValueDetails{
			V: value,
		}
		key2Value[key] = valueDetails
	}
	for key, versionStr := range result2 {
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			return storage.Details{}, err
		}
		if key2Value == nil {
			key2Value = make(map[string]storage.ValueDetails)
		}
		valueDetails, ok := key2Value[key]
		if !ok {
			// Bad data.
			valueDetails.V = "<No-Value>"
		}
		valueDetails.Version = version
		key2Value[key] = valueDetails
	}
	var opaqueVersion interface{}
	if result3 >= 1 {
		opaqueVersion = result3
	}
	return storage.Details{
		Key2Value: key2Value,
		Version:   opaqueVersion,
	}, nil
}

func (rs *RedisStorage) versionKey() string            { return rs.options.Prefix + "Version" }
func (rs *RedisStorage) key2ValueKey() string          { return rs.options.Prefix + "Key2Value" }
func (rs *RedisStorage) key2VersionKey() string        { return rs.options.Prefix + "Key2Version" }
func (rs *RedisStorage) channelName(key string) string { return rs.options.Prefix + key }

func convertError(err error) error {
	switch err {
	case redis.ErrClosed, ErrEventBusClosed:
		return storage.ErrClosed
	default:
		return err
	}
}
