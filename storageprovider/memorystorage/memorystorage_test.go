package memorystorage_test

import (
	"testing"

	"github.com/roy2220/dynconf/storage"
	"github.com/roy2220/dynconf/storageprovider/memorystorage"
)

func TestMemoryStorage(t *testing.T) {
	storage.Test(t, func() storage.Storage { return new(memorystorage.MemoryStorage).Init() })
}
