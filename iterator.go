package kvdb_go

import (
    "bytes"
    "kvdb-go/index"
)

type Iterator struct {
    indexIterator index.Iterator
    db *DB
    options IteratorOptions
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
    indexIterator := db.index.Iterator(options.Reverse)
    iterator := &Iterator{
        indexIterator: indexIterator,
        db: db,
        options: options,
    }
    iterator.skipToNext()
    return iterator
}

func (itr *Iterator) Rewind() {
    itr.indexIterator.Rewind()
    itr.skipToNext()
}

func (itr *Iterator) Seek(key []byte) {
    itr.indexIterator.Seek(key)
    itr.skipToNext()
}

func (itr *Iterator) Next() {
    itr.indexIterator.Next()
    itr.skipToNext()
}

func (itr *Iterator) Valid() bool {
    return itr.indexIterator.Valid()
}

func (itr *Iterator) Key() []byte {
    return itr.indexIterator.Key()
}

func (itr *Iterator) Value() ([]byte, error) {
    valuePos := itr.indexIterator.Value()
    itr.db.mutex.RLock()
    defer itr.db.mutex.RUnlock()
    return itr.db.GetValueByPosition(valuePos)
}

func (itr *Iterator) Close() {
    itr.indexIterator.Close()
}

func (itr *Iterator) skipToNext() {
    prefixLen := len(itr.options.Prefix)
    if prefixLen == 0 {
        return
    }

    for ; itr.indexIterator.Valid(); itr.indexIterator.Next() {
        key := itr.indexIterator.Key()
        if len(key) >= prefixLen && bytes.Equal(itr.options.Prefix, key[:prefixLen]) {
            break
        }
    }
}
