package fio

import (
    "io"
    "path/filepath"
    "testing"
    "github.com/stretchr/testify/assert"
)

func TestMMapIOManagerRead(t *testing.T) {
    path := filepath.Join("/tmp", "test_mmap_io_manager")
    defer destroyFile(path)

    mmapIOManager, err := NewMMapIOManager(path)
    assert.Nil(t, err)

    bytesArr := make([]byte, 10)
    bytesNum, err := mmapIOManager.Read(bytesArr, 0)
    assert.Equal(t, 0, bytesNum)
    assert.Equal(t, io.EOF, err)

    fileIOManager, err := NewFileIOManager(path)
    assert.Nil(t, err)
    _, err = fileIOManager.Write([]byte("key-a"))
    assert.Nil(t, err)
    _, err = fileIOManager.Write([]byte("key-b"))
    assert.Nil(t, err)
    _, err = fileIOManager.Write([]byte("key-c"))
    assert.Nil(t, err)

    mmapIOManager, err = NewMMapIOManager(path)
    assert.Nil(t, err)
    size, err := mmapIOManager.Size()
    assert.Nil(t, err)
    assert.Equal(t, int64(15), size)

    bytesArr = make([]byte, 5)
    bytesNum, err = mmapIOManager.Read(bytesArr, 0)
    assert.Nil(t, err)
    assert.Equal(t, 5, bytesNum)
    assert.Equal(t, "key-a", string(bytesArr))
}