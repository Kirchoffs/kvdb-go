package index

import (
    "kvdb-go/data"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestBTreePut(t *testing.T) {
    bt := NewBTree()

    res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.Nil(t, res1)
    
    res2 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 2, Offset: 2})
    assert.Nil(t, res2)

    res3 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 3, Offset: 3})
    assert.NotNil(t, res3)
    assert.Equal(t, uint32(2), res3.FileId)
    assert.Equal(t, int64(2), res3.Offset)
}

func TestBTreeGet(t *testing.T) {
    bt := NewBTree()
    
    res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.Nil(t, res1)
    pos1 := bt.Get(nil)
    assert.Equal(t, uint32(1), pos1.FileId)
    assert.Equal(t, int64(1), pos1.Offset)

    res2 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 1, Offset: 2})
    assert.Nil(t, res2)
    res3 := bt.Put([]byte("x"), &data.LogRecordPos{FileId: 2, Offset: 3})
    assert.NotNil(t, res3)
    pos3 :=  bt.Get([]byte("x"))
    assert.Equal(t, uint32(2), pos3.FileId)
    assert.Equal(t, int64(3), pos3.Offset)
}

func TestBTreeDelete(t *testing.T) {
    bt := NewBTree()

    res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 1})
    assert.Nil(t, res1)
    res2, ok2 := bt.Delete(nil)
    assert.True(t, ok2)
    assert.Equal(t, uint32(1), res2.FileId)
    assert.Equal(t, int64(1), res2.Offset)

    res3 := bt.Get(nil)
    assert.Equal(t, (*data.LogRecordPos)(nil), res3)

    res4 := bt.Put([]byte("abc"), &data.LogRecordPos{FileId: 2, Offset: 2})
    assert.Nil(t, res4)
    res5, ok5 := bt.Delete([]byte("abc"))
    assert.NotNil(t, res5)
    assert.True(t, ok5)
    assert.Equal(t, uint32(2), res5.FileId)
    assert.Equal(t, int64(2), res5.Offset)

    res6 := bt.Get([]byte("abc"))
    assert.Equal(t, (*data.LogRecordPos)(nil), res6)
}

func TestBTreeIterator(t *testing.T) {
    bt := NewBTree()
    itr := bt.Iterator(false)
    assert.Equal(t, false, itr.Valid())

    bt.Put([]byte("hello"), &data.LogRecordPos{FileId: 1, Offset: 1})
    itr = bt.Iterator(false)
    assert.Equal(t, true, itr.Valid())
    assert.NotNil(t, itr.Key())
    assert.NotNil(t, itr.Value())
    itr.Next()
    assert.Equal(t, false, itr.Valid())

    bt.Put([]byte("world"), &data.LogRecordPos{FileId: 1, Offset: 2})
    bt.Put([]byte("x"), &data.LogRecordPos{FileId: 2, Offset: 1})
    bt.Put([]byte("y"), &data.LogRecordPos{FileId: 2, Offset: 2})
    bt.Put([]byte("z"), &data.LogRecordPos{FileId: 2, Offset: 3})
    itr = bt.Iterator(false)
    var keys [][]byte
    for itr.Rewind(); itr.Valid(); itr.Next() {
        assert.NotNil(t, itr.Key())
        assert.NotNil(t, itr.Value())
        t.Log(itr.Key(), itr.Value())
        keys = append(keys, itr.Key())
    }

    itr = bt.Iterator(true)
    for itr.Rewind(); itr.Valid(); itr.Next() {
        assert.NotNil(t, itr.Key())
        assert.NotNil(t, itr.Value())
        t.Log(itr.Key(), itr.Value())
        assert.Equal(t, keys[len(keys) - 1], itr.Key())
        keys = keys[:len(keys) - 1]
    }

    itr = bt.Iterator(false)
    itr.Seek([]byte("w"))
    t.Log(string(itr.Key()), itr.Value())
    for itr.Seek([]byte("w")); itr.Valid(); itr.Next() {
        assert.NotNil(t, itr.Key())
        assert.NotNil(t, itr.Value())
        t.Log(string(itr.Key()), itr.Value())
    }
    itr.Seek([]byte("zzz"))
    assert.Equal(t, false, itr.Valid())

    itr = bt.Iterator(true)
    itr.Seek([]byte("w"))
    t.Log(string(itr.Key()), itr.Value())
    for itr.Seek([]byte("w")); itr.Valid(); itr.Next() {
        assert.NotNil(t, itr.Key())
        assert.NotNil(t, itr.Value())
        t.Log(string(itr.Key()), itr.Value())
    }
    itr.Seek([]byte("a"))
    assert.Equal(t, false, itr.Valid())
}
