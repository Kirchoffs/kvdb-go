package kvdb_go

import (
    "bytes"
    "kvdb-go/utils"
    "math/rand"
    "os"
    "sort"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
)

func TestDBNewIterator(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-iterator-")
    options.DirPath = dir

    db, err := Open(options)
    defer db.Close()
    assert.Nil(t, err)
    assert.NotNil(t, db)

    iterator := db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    assert.Equal(t, iterator.Valid(), false)
}

func TestDBIteratorWithOneValue(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-iterator-")
    options.DirPath = dir

    db, err := Open(options)
    defer db.Close()
    assert.Nil(t, err)
    assert.NotNil(t, db)

    err = db.Put([]byte("key"), []byte("value"))
    assert.Nil(t, err)

    iterator := db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    assert.Equal(t, iterator.Valid(), true)
    assert.Equal(t, iterator.Key(), []byte("key"))
    value, err := iterator.Value()
    assert.Nil(t, err)
    assert.Equal(t, value, []byte("value"))
}

func TestDBIteratorWithMultiValues(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-iterator-")
    t.Log(dir)
    options.DirPath = dir

    db, err := Open(options)
    defer db.Close()
    assert.Nil(t, err)
    assert.NotNil(t, db)

    iterator := db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    assert.Equal(t, iterator.Valid(), false)

    recordNums := 99

    var randomList []int
    for i := 0; i < recordNums; i++ {
        randomList = append(randomList, i)
    }
    source := rand.NewSource(time.Now().UnixNano())
    random := rand.New(source)
    random.Shuffle(len(randomList), func(i, j int) { randomList[i], randomList[j] = randomList[j], randomList[i] })

    type kv struct {
        key []byte
        value []byte
    }

    var kvList []kv
    for i := 0; i < recordNums; i++ {
        key := utils.GetTestKey(randomList[i])
        value := utils.GetTestValue(5)
        err = db.Put(key, value)
        assert.Nil(t, err)
        kvList = append(kvList, kv {key: key, value: value})
    }

    sort.Slice(kvList, func(i, j int) bool {
        return bytes.Compare(kvList[i].key, kvList[j].key) < 0
    })

    iterator = db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    for i := 0; i < recordNums; i++ {
        assert.Equal(t, iterator.Valid(), true)
        assert.Equal(t, iterator.Key(), kvList[i].key)
        value, err := iterator.Value()
        assert.Nil(t, err)
        assert.Equal(t, value, []byte(kvList[i].value))
        iterator.Next()
    }

    reverseIteratorOptions := IteratorOptions {
        Prefix: nil,
        Reverse: true,
    }
    reverseIterator := db.NewIterator(reverseIteratorOptions)
    assert.NotNil(t, reverseIterator)
    for i := recordNums - 1; i >= 0; i-- {
        assert.Equal(t, reverseIterator.Valid(), true)
        assert.Equal(t, reverseIterator.Key(), kvList[i].key)
        value, err := reverseIterator.Value()
        assert.Nil(t, err)
        assert.Equal(t, value, []byte(kvList[i].value))
        reverseIterator.Next()
    }
}

func TestDBIteratorSeek(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-iterator-")
    t.Log(dir)
    options.DirPath = dir

    db, err := Open(options)
    defer db.Close()
    assert.Nil(t, err)
    assert.NotNil(t, db)

    iterator := db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    assert.Equal(t, iterator.Valid(), false)

    kvs := [][]byte {
        []byte("aaa"),
        []byte("aab"),
        []byte("aba"),
        []byte("acb"),
        []byte("bba"),
        []byte("bbd"),
    }

    for _, kv := range kvs {
        err = db.Put(kv, kv)
        assert.Nil(t, err)
    }

    iterator = db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    iterator.Seek([]byte("abb"))
    for iterator.Valid() {
        key := iterator.Key()
        assert.GreaterOrEqual(t, bytes.Compare(key, []byte("abb")), 0)
        iterator.Next()
    }

    reverseIteratorOptions := IteratorOptions {
        Prefix: nil,
        Reverse: true,
    }
    reverseIterator := db.NewIterator(reverseIteratorOptions)
    assert.NotNil(t, reverseIterator)
    reverseIterator.Seek([]byte("abb"))
    for reverseIterator.Valid() {
        key := reverseIterator.Key()
        assert.LessOrEqual(t, bytes.Compare(key, []byte("abb")), 0)
        reverseIterator.Next()
    }
}

func TestDBIteratorPrefix(t *testing.T) {
    options := DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-iterator-")
    t.Log(dir)
    options.DirPath = dir

    db, err := Open(options)
    defer db.Close()
    assert.Nil(t, err)
    assert.NotNil(t, db)

    iterator := db.NewIterator(DefaultIteratorOptions)
    assert.NotNil(t, iterator)
    assert.Equal(t, iterator.Valid(), false)

    kvs := [][]byte {
        []byte("aaa"),
        []byte("aab"),
        []byte("aba"),
        []byte("acb"),
        []byte("bba"),
        []byte("bbd"),
    }

    for _, kv := range kvs {
        err = db.Put(kv, kv)
        assert.Nil(t, err)
    }

    prefixIteratorOptions := IteratorOptions {
        Prefix: []byte("bb"),
        Reverse: false,
    }

    iterator = db.NewIterator(prefixIteratorOptions)
    assert.NotNil(t, iterator)
    assert.Equal(t, iterator.Valid(), true)
    assert.Equal(t, iterator.Key(), []byte("bba"))
    iterator.Next()
    assert.Equal(t, iterator.Valid(), true)
    assert.Equal(t, iterator.Key(), []byte("bbd"))
}
