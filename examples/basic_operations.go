package main

import (
    "fmt"
    kvdb "kvdb-go"
)

func main() {
    options := kvdb.DefaultOptions
    options.DirPath = "/tmp/kvdb"

    db, err := kvdb.Open(options)
    if err != nil {
        panic(err)
    }

    err = db.Put([]byte("key"), []byte("value"))
    if err != nil {
        panic(err)
    }

    val, err := db.Get([]byte("key"))
    if err != nil {
        panic(err)
    }
    fmt.Println(string(val))

    err = db.Delete([]byte("key"))
    if err != nil {
        panic(err)
    }
}
