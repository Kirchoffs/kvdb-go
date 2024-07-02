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