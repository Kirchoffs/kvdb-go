package index

import (
    "bytes"
    "kvdb-go/data"
    "sort"
    "sync"

    goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
    tree goart.Tree
    lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
    return &AdaptiveRadixTree{
        tree: goart.New(),
        lock: new(sync.RWMutex),
    }
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
    art.lock.Lock()
    defer art.lock.Unlock()
    art.tree.Insert(key, pos)
    return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
    art.lock.RLock()
    defer art.lock.RUnlock()
    value, found := art.tree.Search(key)
    if !found {
        return nil
    }
    return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
    art.lock.Lock()
    defer art.lock.Unlock()
    _, deleted := art.tree.Delete(key)
    return deleted
}

func (art *AdaptiveRadixTree) Size() int {
    art.lock.RLock()
    defer art.lock.RUnlock()
    return art.tree.Size()
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
    art.lock.RLock()
    defer art.lock.RUnlock()

    return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
    return nil
}

type artIterator struct {
    curIndex int
    reverse bool
    values []*Item
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
    values := make([]*Item, tree.Size())
    idx := 0
    if reverse {
        idx = tree.Size() - 1
    }

    saveValues := func (node goart.Node) bool {
        values[idx] = &Item{key: node.Key(), pos: node.Value().(*data.LogRecordPos)}
        if reverse {
            idx--
        } else {
            idx++
        }
        return true
    }
    
    tree.ForEach(saveValues)

    return &artIterator{
        curIndex: 0,
        reverse: reverse,
        values: values,
    }
}

func (ai *artIterator) Rewind() {
    ai.curIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
    ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
        if ai.reverse {
            return bytes.Compare(ai.values[i].key, key) <= 0
        }
        return bytes.Compare(ai.values[i].key, key) >= 0
    })
}

func (ai *artIterator) Next() {
    ai.curIndex++
}

func (ai *artIterator) Valid() bool {
    return ai.curIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
    return ai.values[ai.curIndex].key
}

func (ai *artIterator) Value() *data.LogRecordPos {
    return ai.values[ai.curIndex].pos
}

func (ai *artIterator) Close() {
    ai.values = nil
}

