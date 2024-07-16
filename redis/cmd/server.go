package main

import (
    kvdb "kvdb-go"
    kvdb_redis "kvdb-go/redis"
    "log"
    "sync"

    "github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
    dbs map[int]*kvdb_redis.RedisDataStructure
    server *redcon.Server
    mutex sync.RWMutex
}

func main() {
    redisDataStructure, err := kvdb_redis.NewRedisDataStructure(kvdb.DefaultOptions)
    if err != nil {
        panic(err)
    }

    bitcaskServer := &BitcaskServer{
        dbs: make(map[int]*kvdb_redis.RedisDataStructure),
    }
    bitcaskServer.dbs[0] = redisDataStructure

    bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
    bitcaskServer.listen()
}

func (bitcaskServer *BitcaskServer) listen() {
    log.Println("Starting server at", addr)
    _ = bitcaskServer.server.ListenAndServe()
}

func (bitcaskServer *BitcaskServer) accept(conn redcon.Conn) bool {
    client := new(BitcaskClient)

    bitcaskServer.mutex.Lock()
    defer bitcaskServer.mutex.Unlock()
    
    client.server = bitcaskServer
    client.db = bitcaskServer.dbs[0]
    conn.SetContext(client)

    return true
}

func (bitcaskServer *BitcaskServer) close(conn redcon.Conn, err error) {
    for _, db := range bitcaskServer.dbs {
        _ = db.Close()
    }
    _ = bitcaskServer.server.Close()
}
