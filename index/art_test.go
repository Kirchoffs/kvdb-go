package index

import (
	"kvdb-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRadixTreePut(t *testing.T) {
    art := NewART()

    art.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    art.Put([]byte("key-2"), &data.LogRecordPos{FileId: 1, Offset: 2})
    art.Put([]byte("key-3"), &data.LogRecordPos{FileId: 1, Offset: 3})
}

func TestAdaptiveRadixTreeGet(t *testing.T) {
    art := NewART()

    art.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    pos1 := art.Get([]byte("key-1"))
    assert.NotNil(t, pos1)
    assert.Equal(t, uint32(1), pos1.FileId)
    assert.Equal(t, int64(1), pos1.Offset)

    pos2 := art.Get([]byte("key-2"))
    t.Log(pos2)
    assert.Nil(t, pos2)

    art.Put([]byte("key-1"), &data.LogRecordPos{FileId: 2, Offset: 2})
    pos3 := art.Get([]byte("key-1"))
    assert.NotNil(t, pos3)
    assert.Equal(t, uint32(2), pos3.FileId)
    assert.Equal(t, int64(2), pos3.Offset)
}

func TestAdaptiveRadixTreeDelete(t *testing.T) {
    art := NewART()

    res1 := art.Delete([]byte("key-not-exist"))
    assert.False(t, res1)

    art.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    res2 := art.Delete([]byte("key-1"))
    assert.True(t, res2)
    pos := art.Get([]byte("key-1"))
    assert.Nil(t, pos)
}

func TestAdaptiveRadixTreeSize(t *testing.T) {
    art := NewART()

    assert.Equal(t, 0, art.Size())

    art.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.Equal(t, 1, art.Size())

    art.Put([]byte("key-2"), &data.LogRecordPos{FileId: 1, Offset: 2})
    assert.Equal(t, 2, art.Size())

    art.Put([]byte("key-2"), &data.LogRecordPos{FileId: 1, Offset: 3})
    assert.Equal(t, 2, art.Size())

    art.Delete([]byte("key-1"))
    assert.Equal(t, 1, art.Size())
}

func TestAdaptiveRadixTreeIterator(t *testing.T) {
    art := NewART()

    art.Put([]byte("key-1"), &data.LogRecordPos{FileId: 1, Offset: 1})
    art.Put([]byte("key-2"), &data.LogRecordPos{FileId: 1, Offset: 2})
    art.Put([]byte("key-3"), &data.LogRecordPos{FileId: 1, Offset: 3})
    art.Put([]byte("key-4"), &data.LogRecordPos{FileId: 1, Offset: 4})

    iter := art.Iterator(false)
    for iter.Rewind(); iter.Valid(); iter.Next() {
        assert.NotNil(t, iter.Key())
        assert.NotNil(t, iter.Value())
        t.Log(iter.Key(), iter.Value())
    }
}
