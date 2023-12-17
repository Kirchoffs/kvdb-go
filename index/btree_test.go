package index

import (
    "kvdb-go/data"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestBTreePut(t *testing.T) {
    bt := NewBTree()
    
    res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.True(t, res1)

    res2 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 1, Offset: 2})
    assert.True(t, res2)
}

func TestBTreeGet(t *testing.T) {
    bt := NewBTree()
    
    res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.True(t, res1)
    pos1 := bt.Get(nil)
    assert.Equal(t, uint32(1), pos1.FileId)
    assert.Equal(t, int64(1), pos1.Offset)

    res2 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 1, Offset: 2})
    assert.True(t, res2)
    res3 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 2, Offset: 3})
    assert.True(t, res3)
    pos3 :=  bt.Get([]byte("x"))
    assert.Equal(t, uint32(2), pos3.FileId)
    assert.Equal(t, int64(3), pos3.Offset)
}

func TestBTreeDelete(t *testing.T) {
    bt := NewBTree()

    res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.True(t, res1)
    res2 := bt.Delete(nil)
    assert.True(t, res2)
    res3 := bt.Get(nil)
    assert.Equal(t, (*data.LogRecordPos)(nil), res3)

    res4 := bt.Put([]byte("abc"), &data.LogRecordPos{FileId: 2, Offset: 2})
    assert.True(t, res4)
    res5 := bt.Delete([]byte("abc"))
    assert.True(t, res5)
    res6 := bt.Get([]byte("abc"))
    assert.Equal(t, (*data.LogRecordPos)(nil), res6)
}
