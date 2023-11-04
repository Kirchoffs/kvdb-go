package index

import (
    "bytes"
    "kvdb-go/data"

    "github.com/google/btree"
)

type Indexer interface {
    Put(key []byte, pos *data.LogRecordPos) bool
    Get(key []byte) *data.LogRecordPos
    Delete(key []byte) bool
}

type IndexType = int8

const (
    BTreeIndex IndexType = iota + 1
    ARTIndex
)

func NewIndexer(indexType IndexType) Indexer {
    switch indexType {
    case BTreeIndex:
        return NewBTree()
    case ARTIndex:
        return nil
    default:
        panic("unsupported index type")
    }
}

type Item struct {
    key []byte
    pos *data.LogRecordPos
}

func (x *Item) Less(y btree.Item) bool {
    return bytes.Compare(x.key, y.(*Item).key) == -1
}
