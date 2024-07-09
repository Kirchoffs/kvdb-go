package redis

import (
	"encoding/binary"
	"errors"
	kvdb "kvdb-go"
	"time"
)

type redisDataType = byte
const (
    String redisDataType = iota
    Hash
    Set
    List
    ZSet
)

var (
    ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)


type RedisDataStructure struct {
    db *kvdb.DB
}

func NewRedisDataStructure(options kvdb.Options) (*RedisDataStructure, error) {
    db, err := kvdb.Open(options)
    if err != nil {
        return nil, err
    }

    return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
    if value == nil {
        return nil
    }

    buffer := make([]byte, binary.MaxVarintLen64 + 1)
    buffer[0] = String
    var idx = 1
    var expire int64 = 0
    if ttl != 0 {
        expire = time.Now().Add(ttl).UnixNano()
    }
    idx += binary.PutVarint(buffer[idx:], expire)

    encodedeValue := make([]byte, idx + len(value))
    copy(encodedeValue, buffer[:idx])
    copy(encodedeValue[idx:], value)

    return rds.db.Put(key, encodedeValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
    value, err := rds.db.Get(key)
    if err != nil {
        return nil, err
    }

    dataType := value[0]
    if dataType != String {
        return nil, ErrWrongTypeOperation
    }

    var idx = 1
    expire, n := binary.Varint(value[idx:])
    idx += n
    if expire > 0 && expire <= time.Now().UnixNano() {
        return nil, nil
    }

    return value[idx:], nil
}

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
    meta, err := rds.findMetadata(key, Hash)
    if err != nil {
        return false, err
    }

    hk := &hashInternalKey {
        key: key,
        version: meta.version,
        field: field,
    }
    encodedHk := hk.encode()

    var exist bool = true
    if _, err = rds.db.Get(encodedHk); err != nil {
        if err == kvdb.ErrKeyNotFound {
            exist = false
        } else {
            return false, err
        }
    }

    wb := rds.db.NewWriteBatch(kvdb.DefaultWriteBatchOptions)
    if !exist {
        meta.size++
        _ = wb.Put(key, meta.encode())
    }
    _ = wb.Put(encodedHk, value)
    if err = wb.Commit(); err != nil {
        return false, err
    }
    
    return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
    meta, err := rds.findMetadata(key, Hash)
    if err != nil {
        return nil, err
    }
    if meta.size == 0 {
        return nil, kvdb.ErrKeyNotFound
    }

    hk := &hashInternalKey {
        key: key,
        version: meta.version,
        field: field,
    }
    encodedHk := hk.encode()

    return rds.db.Get(encodedHk)
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
    meta, err := rds.findMetadata(key, Hash)
    if err != nil {
        return false, err
    }

    if meta.size == 0 {
        return false, nil
    }

    hk := &hashInternalKey {
        key: key,
        version: meta.version,
        field: field,
    }
    encodedHk := hk.encode()

    var exist bool = true
    if _, err = rds.db.Get(encodedHk); err == kvdb.ErrKeyNotFound {
        exist = false
    }

    if exist {
        wb := rds.db.NewWriteBatch(kvdb.DefaultWriteBatchOptions)
        meta.size--
        _ = wb.Put(key, meta.encode())
        _ = wb.Delete(encodedHk)
        if err = wb.Commit(); err != nil {
            return false, err
        }
    }

    return exist, nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
    metaBuffer, err := rds.db.Get(key)
    if err != nil && err != kvdb.ErrKeyNotFound {
        return nil, err
    }

    var meta *metadata
    var exist bool = true
    if err == kvdb.ErrKeyNotFound {
        exist = false
    } else {
        meta = decode(metaBuffer)
        if meta.dataType != dataType {
            return nil, ErrWrongTypeOperation
        }

        if meta.expire > 0 && meta.expire <= time.Now().UnixNano() {
            exist = false
        }
    }

    if !exist {
        meta = &metadata {
            dataType: dataType,
            expire: 0,
            version: time.Now().UnixNano(),
            size: 0,
        }

        if dataType == List {
            meta.head = initialListMark
            meta.tail = initialListMark
        }
    }

    return meta, nil
}
