package utils

import (
    "fmt"
    "math/rand"
    "time"
)

var (
    randStr = rand.New(rand.NewSource(time.Now().Unix()))
    letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func GetTestKey(i int) []byte {
    return []byte(fmt.Sprintf("kvdb-test-key-%09d", i))
}

func GetTestValue(n int) []byte {
    b := make([]byte, n)
    for i := range b {
        b[i] = letters[randStr.Intn(len(letters))]
    }

    return []byte(fmt.Sprintf("kvdb-test-value-%s", b))
}