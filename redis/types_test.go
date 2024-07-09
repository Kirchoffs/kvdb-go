package redis

import (
	kvdb "kvdb-go"
	"kvdb-go/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructureGet(t *testing.T) {
    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-redis-get-")
    options.DirPath = dir

    rds, err := NewRedisDataStructure(options)
    assert.Nil(t, err)

    err = rds.Set(utils.GetTestKey(1), 0, utils.GetTestValue(42))
    assert.Nil(t, err)

    err = rds.Set(utils.GetTestKey(32), time.Second * 5, utils.GetTestValue(2038))
    assert.Nil(t, err)

    val1, err := rds.Get(utils.GetTestKey(1))
    assert.Nil(t, err)
    assert.NotNil(t, val1)

    val2, err := rds.Get(utils.GetTestKey(32))
    assert.Nil(t, err)
    assert.NotNil(t, val2)

    val3, err := rds.Get(utils.GetTestKey(99))
    assert.NotNil(t, err)
    assert.Nil(t, val3)
}

func TestRedisDataStructureHGet(t *testing.T) {
    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-redis-hget-")
    options.DirPath = dir

    rds, err := NewRedisDataStructure(options)
    assert.Nil(t, err)

    val1 := utils.GetTestValue(42)
    ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field-alpha"), val1)
    assert.Nil(t, err)
    assert.True(t, ok1)

    val2 := utils.GetTestValue(89)
    ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field-alpha"), val2)
    assert.Nil(t, err)
    assert.False(t, ok2)

    val3 := utils.GetTestValue(42)
    ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field-beta"), val3)
    assert.Nil(t, err)
    assert.True(t, ok3)

    actualVal2, _ := rds.HGet(utils.GetTestKey(1), []byte("field-alpha"))
    assert.Equal(t, val2, actualVal2)

    actualVal3, _ := rds.HGet(utils.GetTestKey(1), []byte("field-beta"))
    assert.Equal(t, val3, actualVal3)

    _, err = rds.HGet(utils.GetTestKey(2), []byte("field-alpha"))
    assert.NotNil(t, err)
}

func TestRedisDataStructureHDel(t *testing.T) {
    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-redis-hdel-")
    options.DirPath = dir

    rds, err := NewRedisDataStructure(options)
    assert.Nil(t, err)

    val1 := utils.GetTestValue(42)
    ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field-alpha"), val1)
    assert.Nil(t, err)
    assert.True(t, ok1)

    val2 := utils.GetTestValue(89)
    ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field-beta"), val2)
    assert.Nil(t, err)
    assert.True(t, ok2)

    ok3, err := rds.HDel(utils.GetTestKey(1), []byte("field-alpha"))
    assert.Nil(t, err)
    assert.True(t, ok3)

    _, err = rds.HGet(utils.GetTestKey(1), []byte("field-alpha"))
    assert.NotNil(t, err)

    ok4, err := rds.HDel(utils.GetTestKey(1), []byte("field-alpha"))
    assert.Nil(t, err)
    assert.False(t, ok4)

    ok5, err := rds.HDel(utils.GetTestKey(1), []byte("field-beta"))
    assert.Nil(t, err)
    assert.True(t, ok5)

    _, err = rds.HGet(utils.GetTestKey(1), []byte("field-beta"))
    assert.NotNil(t, err)

    ok6, err := rds.HDel(utils.GetTestKey(1), []byte("field-beta"))
    assert.Nil(t, err)
    assert.False(t, ok6)
}