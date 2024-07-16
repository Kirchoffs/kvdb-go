package benchmark

import (
    kvdb "kvdb-go"
    "kvdb-go/utils"
    "math/rand"
    "os"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
)

var db *kvdb.DB

func init() {
    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-benchmark-")
    options.DirPath = dir

    var err error
    db, err = kvdb.Open(options)
    if err != nil {
        panic(err)
    }
}

func BenchmarkPut(b *testing.B) {
    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
        assert.Nil(b, err)
    }
}

func BenchmarkGet(b *testing.B) {
    for i := 0; i < 10000; i++ {
        err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
        assert.Nil(b, err)
    }

    rng := rand.New(rand.NewSource(time.Now().Unix()))
    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        _, err := db.Get(utils.GetTestKey(rng.Int()))
        if err != nil && err != kvdb.ErrKeyNotFound {
            b.Fatal(err)
        }
    }
}

func BenchmarkDelete(b *testing.B) {
    for i := 0; i < 10000; i++ {
        err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
        assert.Nil(b, err)
    }

    rng := rand.New(rand.NewSource(time.Now().Unix()))
    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        err := db.Delete(utils.GetTestKey(rng.Int()))
        if err != nil && err != kvdb.ErrKeyNotFound {
            b.Fatal(err)
        }
    }
}
