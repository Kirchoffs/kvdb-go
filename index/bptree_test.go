package index

import (
	"kvdb-go/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTreePut(t *testing.T) {
    path := filepath.Join(os.TempDir(), "bptree-put")
    _ = os.MkdirAll(path, os.ModePerm)
    defer func() {
        _ = os.RemoveAll(path)
    }()
    
    tree := NewBPlusTree(path, false)

    res1 := tree.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.True(t, res1)
    res2 := tree.Put([]byte("key-2"), &data.LogRecordPos{FileId: 2, Offset: 2})
    assert.True(t, res2)
    res3 := tree.Put([]byte("key-3"), &data.LogRecordPos{FileId: 3, Offset: 3})
    assert.True(t, res3)
}

func TestBPlusTreeGet(t *testing.T) {
    path := filepath.Join(os.TempDir(), "bptree-get")
    _ = os.MkdirAll(path, os.ModePerm)
    defer func() {
        _ = os.RemoveAll(path)
    }()

    tree := NewBPlusTree(path, false)

    posNotExists := tree.Get([]byte("key-not-exists"))
    assert.Nil(t, posNotExists)

    tree.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    pos1 := tree.Get([]byte("key-1"))
    assert.NotNil(t, pos1)

    tree.Put([]byte("key-2"), &data.LogRecordPos{FileId: 2, Offset: 2})
    pos2 := tree.Get([]byte("key-2"))
    assert.NotNil(t, pos2)
}

func TestBPlusTreeDelete(t *testing.T) {
    path := filepath.Join(os.TempDir(), "bptree-delete")
    _ = os.MkdirAll(path, os.ModePerm)
    defer func() {
        _ = os.RemoveAll(path)
    }()

    tree := NewBPlusTree(path, false)

    res1 := tree.Delete([]byte("key-not-exists"))
    assert.False(t, res1)

    tree.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    res2 := tree.Delete([]byte("key-1"))
    assert.True(t, res2)

    res3 := tree.Delete([]byte("key-1"))
    assert.False(t, res3)
}
