package kvdb_go

import (
    "kvdb-go/utils"
    "os"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestDBWriteBatch(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-batch-")
    options.DirPath = dir
    db, err := Open(options)
    defer destroyDB(db)
    assert.Nil(t, err)
    assert.NotNil(t, db)

    wb := db.NewWriteBatch(DefaultWriteBatchOptions)
    err = wb.Put(utils.GetTestKey(1), utils.GetTestValue(24))
    assert.Nil(t, err)
    err = wb.Delete(utils.GetTestKey(2))
    assert.Nil(t, err)

    _, err = db.Get(utils.GetTestKey(1))
    assert.Equal(t, ErrKeyNotFound, err)

    err = wb.Commit()
    assert.Nil(t, err)

    val, err := db.Get(utils.GetTestKey(1))
    assert.NotNil(t, val)
    assert.Nil(t, err)

    wb = db.NewWriteBatch(DefaultWriteBatchOptions)
    err = wb.Delete(utils.GetTestKey(1))
    assert.Nil(t, err)
    err = wb.Commit()
    assert.Nil(t, err)

    _, err = db.Get(utils.GetTestKey(1))
    assert.Equal(t, ErrKeyNotFound, err)
}

func TestDBWriteBatchReboot(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-batch-reboot-")
    options.DirPath = dir
    db, err := Open(options)
    defer destroyDB(db)
    assert.Nil(t, err)
    assert.NotNil(t, db)

    err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
    assert.Nil(t, err)

    wb := db.NewWriteBatch(DefaultWriteBatchOptions)
    err = wb.Put(utils.GetTestKey(2), utils.GetTestValue(24))
    assert.Nil(t, err)
    err = wb.Delete(utils.GetTestKey(1))
    assert.Nil(t, err)

    err = wb.Commit()  // First commit 
    assert.Nil(t, err)

    err = wb.Put(utils.GetTestKey(101), utils.GetTestValue(24))
    assert.Nil(t, err)
    err = wb.Commit()  // Second commit
    assert.Nil(t, err)

    err = db.Close()
    assert.Nil(t, err)

    db, err = Open(options)
    assert.Nil(t, err)

    _, err = db.Get(utils.GetTestKey(1))
    assert.Equal(t, ErrKeyNotFound, err)

    assert.Equal(t, uint64(2), db.seqNum)
}
