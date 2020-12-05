module github.com/roy2220/dynconf/storageprovider/redisstorage

go 1.14

require (
	github.com/alicebob/miniredis/v2 v2.14.1
	github.com/go-redis/redis/v8 v8.4.0
	github.com/roy2220/dynconf v0.0.0
	github.com/stretchr/testify v1.6.1
)

replace github.com/roy2220/dynconf => ../..
