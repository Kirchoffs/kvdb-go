package fio

import (
    "os"
    "path/filepath"
    "testing"
    "github.com/stretchr/testify/assert"
)

func destroyFile(name string) {
    if _, err := os.Stat(name); err == nil {
        if err := os.Remove(name); err != nil {
            panic(err)
        }
    }
}

func TestNewFileIOManager(t *testing.T) {
    path := filepath.Join("/tmp", "test_file_io_manager")
    destroyFile(path)

    fio, err := NewFileIOManager(path)
    assert.Nil(t, err)
    assert.NotNil(t, fio)

    destroyFile(path)
}

func TestFileIOWrite(t *testing.T) {
    path := filepath.Join("/tmp", "test_file_io_manager")
    destroyFile(path)

    fio, err := NewFileIOManager(path)
    assert.Nil(t, err)
    assert.NotNil(t, fio)

    n, err := fio.Write([]byte("hello world"))
    assert.Nil(t, err)
    assert.Equal(t, 11, n)

    n, err = fio.Write([]byte(""))
    assert.Nil(t, err)
    assert.Equal(t, 0, n)

    destroyFile(path)
}

func TestFileIORead(t *testing.T) {
    path := filepath.Join("/tmp", "test_file_io_manager")
    destroyFile(path)

    fio, err := NewFileIOManager(path)
    assert.Nil(t, err)
    assert.NotNil(t, fio)

    _, err = fio.Write([]byte("key-a"))
    assert.Nil(t, err)

    _, err = fio.Write([]byte("key-b"))
    assert.Nil(t, err)

    keyA := make([]byte, 5)
    n, err := fio.Read(keyA, 0)
    assert.Nil(t, err)
    assert.Equal(t, 5, n)
    assert.Equal(t, "key-a", string(keyA))

    keyB := make([]byte, 5)
    n, err = fio.Read(keyB, 5)
    assert.Nil(t, err)
    assert.Equal(t, 5, n)
    assert.Equal(t, []byte("key-b"), keyB)

    destroyFile(path)
}

func TestFileIOSync(t *testing.T) {
    path := filepath.Join("/tmp", "test_file_io_manager")
    destroyFile(path)

    fio, err := NewFileIOManager(path)
    assert.Nil(t, err)
    assert.NotNil(t, fio)

    _, err = fio.Write([]byte("hello world"))
    assert.Nil(t, err)

    err = fio.Sync()
    assert.Nil(t, err)

    destroyFile(path)
}
