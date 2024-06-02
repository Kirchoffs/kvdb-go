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
    Size() int
    Iterator(reverse bool) Iterator
    Close() error
}

type IndexType = int8

const (
    BTreeIndex IndexType = iota + 1
    ARTIndex
    BPTreeIndex
)

func NewIndexer(indexType IndexType, dirPath string, sync bool) Indexer {
    switch indexType {
    case BTreeIndex:
        return NewBTree()
    case ARTIndex:
        return NewART()
    case BPTreeIndex:
        return NewBPlusTree(dirPath, sync)
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

type Iterator interface {
    Rewind()
    Seek(key []byte)
    Next()
    Valid() bool
    Key() []byte
    Value() *data.LogRecordPos
    Close()
}
