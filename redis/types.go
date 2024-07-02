package redis

import (
	"encoding/binary"
	"errors"
	kvdb "kvdb-go"
	"time"
)

type redisDataTypes = byte
const (
    String redisDataTypes = iota
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
