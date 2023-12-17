package kvdb_go

import (
    "kvdb-go/utils"
    "os"
    "testing"

    "github.com/sirupsen/logrus"
    "github.com/stretchr/testify/assert"
)

func init() {
    logrus.SetOutput(os.Stdout)
    logrus.SetLevel(logrus.DebugLevel)
}

func destroyDB(db *DB) {
    if db != nil {
        if db.activeFile != nil {
            _ = db.Close()
        }
        err := os.RemoveAll(db.options.DirPath)
        if err != nil {
            panic(err)
        }
    }    
}

func TestOpen(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go")
    options.DirPath = dir
    db, err := Open(options)
    defer destroyDB(db)
    assert.Nil(t, err)
    assert.NotNil(t, db)
}

func TestDBPut(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-put-")
    t.Log(dir)

    options.DirPath = dir
    options.DataFileSize = 64 * 1024 * 1024
    db, err := Open(options)
    defer destroyDB(db)
    assert.Nil(t, err)
    assert.NotNil(t, db)

    // 1. Create a data entry
    err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
    assert.Nil(t, err)
    val1, err := db.Get(utils.GetTestKey(1))
    assert.Nil(t, err)
    assert.NotNil(t, val1)

    // 2. Update a data entry
    err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
    assert.Nil(t, err)
    val2, err := db.Get(utils.GetTestKey(1))
    assert.Nil(t, err)
    assert.NotNil(t, val2)

    // 3. Empty key
    err = db.Put(nil, utils.GetTestValue(24))
    assert.Equal(t, ErrKeyIsEmpty, err)

    // 4. Empty value
    err = db.Put(utils.GetTestKey(22), nil)
    assert.Nil(t, err)
    val3, err := db.Get(utils.GetTestKey(22))
    assert.Equal(t, 0, len(val3))
    assert.Nil(t, err)

    // 5. Write large data to trigger multiple data files
    for i := 0; i < 1000000; i++ {
        err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
        assert.Nil(t, err)
    }
    assert.Equal(t, 2, len(db.olderFiles))

    // 6. Restart database
    err = db.Close()
    assert.Nil(t, err)

    db2, err := Open(options)
    assert.Nil(t, err)
    assert.NotNil(t, db2)
    val4 := utils.GetTestValue(128)
    err = db2.Put(utils.GetTestKey(55), val4)
    assert.Nil(t, err)
    val5, err := db2.Get(utils.GetTestKey(55))
    assert.Nil(t, err)
    assert.Equal(t, val4, val5)
}

func TestDBGet(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-get")
    options.DirPath = dir
    options.DataFileSize = 64 * 1024 * 1024
    db, err := Open(options)
    defer destroyDB(db)
    assert.Nil(t, err)
    assert.NotNil(t, db)

    // 1. Read an existing key
    err = db.Put(utils.GetTestKey(11), utils.GetTestValue(24))
    assert.Nil(t, err)
    val1, err := db.Get(utils.GetTestKey(11))
    assert.Nil(t, err)
    assert.NotNil(t, val1)

    // 2. Read a non-existing key
    val2, err := db.Get([]byte("some key unknown"))
    assert.Nil(t, val2)
    assert.Equal(t, ErrKeyNotFound, err)

    // 3. Read after update
    err = db.Put(utils.GetTestKey(22), utils.GetTestValue(24))
    assert.Nil(t, err)
    err = db.Put(utils.GetTestKey(22), utils.GetTestValue(24))
    val3, err := db.Get(utils.GetTestKey(22))
    assert.Nil(t, err)
    assert.NotNil(t, val3)

    // 4. Read after delete
    err = db.Put(utils.GetTestKey(33), utils.GetTestValue(24))
    assert.Nil(t, err)
    err = db.Delete(utils.GetTestKey(33))
    assert.Nil(t, err)
    val4, err := db.Get(utils.GetTestKey(33))
    assert.Equal(t, 0, len(val4))
    assert.Equal(t, ErrKeyNotFound, err)

    // 5. Read after multiple data files
    for i := 100; i < 1000000; i++ {
        err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
        assert.Nil(t, err)
    }
    assert.Equal(t, 2, len(db.olderFiles))
    val5, err := db.Get(utils.GetTestKey(101))
    assert.Nil(t, err)
    assert.NotNil(t, val5)

    // 6. Restart database
    err = db.Close()
    assert.Nil(t, err)

    db2, err := Open(options)
    val6, err := db2.Get(utils.GetTestKey(11))
    assert.Nil(t, err)
    assert.NotNil(t, val6)
    assert.Equal(t, val1, val6)

    val7, err := db2.Get(utils.GetTestKey(22))
    assert.Nil(t, err)
    assert.NotNil(t, val7)
    assert.Equal(t, val3, val7)

    val8, err := db2.Get(utils.GetTestKey(33))
    assert.Equal(t, 0, len(val8))
    assert.Equal(t, ErrKeyNotFound, err)
}

func TestDBDelete(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-delete")
    options.DirPath = dir
    options.DataFileSize = 64 * 1024 * 1024
    db, err := Open(options)
    defer destroyDB(db)
    assert.Nil(t, err)
    assert.NotNil(t, db)

    // 1. Delete an existing key
    err = db.Put(utils.GetTestKey(11), utils.GetTestValue(128))
    assert.Nil(t, err)
    err = db.Delete(utils.GetTestKey(11))
    assert.Nil(t, err)
    _, err = db.Get(utils.GetTestKey(11))
    assert.Equal(t, ErrKeyNotFound, err)

    // 2. Delete a non-existing key
    err = db.Delete([]byte("unknown key"))
    assert.Nil(t, err)

    // 3. Delete an empty key
    err = db.Delete(nil)
    assert.Equal(t, ErrKeyIsEmpty, err)

    // 4. Delete after update
    err = db.Put(utils.GetTestKey(22), utils.GetTestValue(128))
    assert.Nil(t, err)
    err = db.Delete(utils.GetTestKey(22))
    assert.Nil(t, err)

    err = db.Put(utils.GetTestKey(22), utils.GetTestValue(128))
    assert.Nil(t, err)
    val1, err := db.Get(utils.GetTestKey(22))
    assert.NotNil(t, val1)
    assert.Nil(t, err)

    // 5. Restart database
    err = db.Close()
    assert.Nil(t, err)

    db2, err := Open(options)
    _, err = db2.Get(utils.GetTestKey(11))
    assert.Equal(t, ErrKeyNotFound, err)

    val2, err := db2.Get(utils.GetTestKey(22))
    assert.Nil(t, err)
    assert.Equal(t, val1, val2)
}
