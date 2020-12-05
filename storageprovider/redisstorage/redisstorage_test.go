package redisstorage_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/roy2220/dynconf/storage"
	"github.com/roy2220/dynconf/storageprovider/redisstorage"
	"github.com/stretchr/testify/assert"
)

func TestRedisStorage(t *testing.T) {
	storage.Test(t, func() storage.Storage { return newRedisStorage(t) })
}

type redisStorage struct {
	*redisstorage.RedisStorage

	m *miniredis.Miniredis
}

func newRedisStorage(t *testing.T) redisStorage {
	m, err := miniredis.Run()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	client := redis.NewClient(&redis.Options{
		Addr:     m.Addr(),
		PoolSize: 200,
	})
	return redisStorage{
		RedisStorage: new(redisstorage.RedisStorage).Init(client, redisstorage.Options{}),
		m:            m,
	}
}

func (rs redisStorage) Close() error {
	err := rs.RedisStorage.Close()
	rs.m.Close()
	return err
}
