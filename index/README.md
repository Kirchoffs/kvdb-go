# Notes

## Details
- When returning an iterator, we need to have a read lock on the DB.

- Iterator  
```
type Iterator interface {
    Rewind()
    Seek(key []byte)
    Next()
    Valid() bool
    Key() []byte
    Value() *data.LogRecordPos
    Close()
}
```

```
for itr.Rewind(); itr.Valid(); itr.Next() {
    assert.NotNil(t, itr.Key())
    assert.NotNil(t, itr.Value())
}
```