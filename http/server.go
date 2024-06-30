package main

import (
    "encoding/json"
    "fmt"
    kvdb "kvdb-go"
    "log"
    "net/http"
    "os"
)

var db *kvdb.DB

func init() {
    var err error

    options := kvdb.DefaultOptions
    dir, _ := os.MkdirTemp("", "kvdb-go-server-")
    log.Printf("db dir: %s", dir)
    options.DirPath = dir
    db, err = kvdb.Open(options)
    if err != nil {
        panic(fmt.Sprintf("failed to open db: %v", err))
    }
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
    if request.Method != http.MethodPost {
        http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
        return
    }

    var data map[string]string
    if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
        http.Error(writer, err.Error(), http.StatusBadRequest)
        return
    }

    for key, value := range data {
        if err := db.Put([]byte(key), []byte(value)); err != nil {
            http.Error(writer, err.Error(), http.StatusInternalServerError)
            log.Printf("failed to put key-value: %v", err)
            return
        }
    }
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
    if request.Method != http.MethodGet {
        http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
        return
    }

    key := request.URL.Query().Get("key")
    value, err := db.Get([]byte(key))
    if err != nil && err != kvdb.ErrKeyNotFound {
        http.Error(writer, err.Error(), http.StatusInternalServerError)
        log.Printf("failed to get value: %v", err)
        return
    }

    writer.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
    if request.Method != http.MethodDelete {
        http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
        return
    }

    key := request.URL.Query().Get("key")
    if err := db.Delete([]byte(key)); err != nil && err != kvdb.ErrKeyNotFound {
        http.Error(writer, err.Error(), http.StatusInternalServerError)
        log.Printf("failed to delete key: %v", err)
        return
    }

    writer.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(writer).Encode(string("OK"))
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
    if request.Method != http.MethodGet {
        http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
        return
    }

    keys := db.ListKeys()
    var result []string
    for _, key := range keys {
        result = append(result, string(key))
    }

    writer.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
    if request.Method != http.MethodGet {
        http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
        return
    }

    stats := db.Stat()
    writer.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(writer).Encode(stats)
}

func main() {
    http.HandleFunc("/kvdb/put", handlePut)
    http.HandleFunc("/kvdb/get", handleGet)
    http.HandleFunc("/kvdb/delete", handleDelete)
    http.HandleFunc("/kvdb/list", handleListKeys)
    http.HandleFunc("/kvdb/stat", handleStat)

    _ = http.ListenAndServe("localhost:8080", nil)
}
