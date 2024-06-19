package index

import (
    "bytes"
    "kvdb-go/data"
    "sort"
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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
    item := &Item{key: key, pos: pos}
    bt.lock.Lock()
    oldItem := bt.tree.ReplaceOrInsert(item)
    bt.lock.Unlock()
    if oldItem == nil {
        return nil;
    }
    return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
    itemKey := &Item{key: key}
    
    item := bt.tree.Get(itemKey)

    if item == nil {
        return nil
    }
    return item.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
    itemKey := &Item{key: key}
    bt.lock.Lock()
    oldItem := bt.tree.Delete(itemKey)
    bt.lock.Unlock()
    if oldItem == nil {
        return nil, false
    }
    return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
    bt.lock.RLock()
    defer bt.lock.RUnlock()
    return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
    if bt.tree == nil {
        return nil
    }

    bt.lock.RLock()
    defer bt.lock.RUnlock()
    return newBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
    return nil
}

type btreeIterator struct {
    curIndex int
    reverse bool
    values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
    var idx int
    values := make([]*Item, tree.Len())

    saveValues := func(it btree.Item) bool {
        values[idx] = it.(*Item)
        idx++
        return true
    }

    if reverse {
        tree.Descend(saveValues)
    } else {
        tree.Ascend(saveValues)
    }

    return &btreeIterator{
        curIndex: 0,
        reverse: reverse,
        values: values,
    }
}

func (bti *btreeIterator) Rewind() {
    bti.curIndex = 0
}

func (bti *btreeIterator) Seek(key []byte) {
    bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
        if bti.reverse {
            return bytes.Compare(bti.values[i].key, key) <= 0
        }
        return bytes.Compare(bti.values[i].key, key) >= 0
    })
}

func (bti *btreeIterator) Next() {
    bti.curIndex++
}

func (bti *btreeIterator) Valid() bool {
    return bti.curIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
    return bti.values[bti.curIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
    return bti.values[bti.curIndex].pos
}

func (bti *btreeIterator) Close() {
    bti.values = nil
}
