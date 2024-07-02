package redis

import (
	kvdb "kvdb-go"
	"kvdb-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructureDel(t *testing.T) {
    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-redis-del-")
    options.DirPath = dir

    rds, err := NewRedisDataStructure(options)
    assert.Nil(t, err)

    _, err = rds.Get(utils.GetTestKey(1))
    assert.NotNil(t, err)
    assert.Equal(t, kvdb.ErrKeyNotFound, err)

    err = rds.Set(utils.GetTestKey(1), 0, utils.GetTestValue(42))
    assert.Nil(t, err)

    err = rds.Del(utils.GetTestKey(1))
    assert.Nil(t, err)

    _, err = rds.Get(utils.GetTestKey(1))
    assert.NotNil(t, err)
}

func TestRedisDataStructureType(t *testing.T) {
    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-redis-type-")
    options.DirPath = dir

    rds, err := NewRedisDataStructure(options)
    assert.Nil(t, err)

    _, err = rds.Get(utils.GetTestKey(1))
    assert.NotNil(t, err)
    assert.Equal(t, kvdb.ErrKeyNotFound, err)

    err = rds.Set(utils.GetTestKey(1), 0, utils.GetTestValue(42))
    assert.Nil(t, err)

    typ, err := rds.Type(utils.GetTestKey(1))
    assert.Nil(t, err)
    assert.Equal(t, String, typ)
}
