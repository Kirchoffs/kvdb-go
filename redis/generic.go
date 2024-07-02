package redis

import "errors"

func (rds *RedisDataStructure) Del(key []byte) error {
    return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataTypes, error) {
    encodedValue, err := rds.db.Get(key)
    if err != nil {
        return 0, err
    }

    if len(encodedValue) == 0 {
        return 0, errors.New("value is null")
    }

    return redisDataTypes(encodedValue[0]), nil
}
