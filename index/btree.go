package index

import (
    "kvdb-go/data"
    "sync"

    "github.com/google/btree"
)

type BTree struct {
    tree *btree.BTree
    lock *sync.RWMutex
}

func NewBTree() *BTree {
    return &BTree{
        tree: btree.New(32),
        lock: new(sync.RWMutex),
    }
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
    item := &Item{key: key, pos: pos}
    bt.lock.Lock()
    bt.tree.ReplaceOrInsert(item)
    bt.lock.Unlock()
    return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
    itemKey := &Item{key: key}
    
    item := bt.tree.Get(itemKey)

    if item == nil {
        return nil
    }
    return item.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
    itemKey := &Item{key: key}
    bt.lock.Lock()
    oldItem := bt.tree.Delete(itemKey)
    bt.lock.Unlock()
    return oldItem != nil
}
