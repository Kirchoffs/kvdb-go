package kvdb_go

import (
    "io"
    "kvdb-go/data"
    "kvdb-go/index"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
)

type DB struct {
    options Options
    mutex *sync.RWMutex
    fileIds []uint32
    activeFile *data.DataFile
    olderFiles map[uint32]*data.DataFile
    index index.Indexer
    seqNum uint64
}

func Open(options Options) (*DB, error) {
    if err := checkOptions(options); err != nil {
        return nil, err
    }

    if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
        if err := os.MkdirAll(options.DirPath, 0755); err != nil {
            return nil, err
        }
    }

    db := &DB {
        options: options,
        mutex: new(sync.RWMutex),
        olderFiles: make(map[uint32]*data.DataFile),
        index: index.NewIndexer(options.IndexType),
    }

    if err := db.loadDataFiles(); err != nil {
        return nil, err
    }

    if err := db.loadIndexFromDataFiles(); err != nil {
        return nil, err
    }

    return db, nil
}

func (db *DB) Close() error {
    if db.activeFile == nil {
        return nil
    }

    db.mutex.Lock()
    defer db.mutex.Unlock()

    if err := db.activeFile.Close(); err != nil {
        return err
    }

    for _, file := range db.olderFiles {
        if err := file.Close(); err != nil {
            return err
        }
    }

    return nil
}

func (db *DB) Sync() error {
    if db.activeFile == nil {
        return nil
    }

    db.mutex.Lock()
    defer db.mutex.Unlock()

    return db.activeFile.Sync()
}

func (db *DB) Put(key []byte, value []byte) error {
    if len(key) == 0 {
        return ErrKeyIsEmpty
    }

    logRecord := &data.LogRecord {
        Key: logRecordKeyWithSeq(key, nonTransactionSeqNum),
        Value: value,
        Type: data.LogRecordNormal,
    }

    if pos, err := db.appendLogRecordWithLock(logRecord); err != nil {
        return err
    } else {
        if ok := db.index.Put(key, pos); !ok {
            return ErrIndexUpdateFailed
        }
        return nil
    }
}

func (db *DB) Get(key []byte) ([]byte, error) {
    db.mutex.RLock()
    defer db.mutex.RUnlock()

    if len(key) == 0 {
        return nil, ErrKeyIsEmpty
    }

    logRecordPos := db.index.Get(key)
    if logRecordPos == nil {
        return nil, ErrKeyNotFound
    }

    return db.GetValueByPosition(logRecordPos)
}

func (db *DB) GetValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
    var dataFile *data.DataFile
    if db.activeFile.FileId == logRecordPos.FileId {
        dataFile = db.activeFile
    } else {
        dataFile = db.olderFiles[logRecordPos.FileId]
    }

    if dataFile == nil {
        return nil, ErrDataFileNotFound
    }

    logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
    if err != nil {
        return nil, nil
    }

    if logRecord.Type == data.LogRecordDeleted {
        return nil, ErrKeyNotFound
    }

    return logRecord.Value, nil
}

func (db *DB) ListKeys() [][]byte {
    iterator := db.NewIterator(DefaultIteratorOptions)
    keys := make([][]byte, db.index.Size())
    var idx int
    for iterator.Rewind(); iterator.Valid(); iterator.Next() {
        keys[idx] = iterator.Key()
        idx++
    }
    return keys
}

func (db *DB) Delete(key []byte) error {
    if len(key) == 0 {
        return ErrKeyIsEmpty
    }

    if pos := db.index.Get(key); pos == nil {
        return nil
    }

    logRecord := &data.LogRecord {
        Key: logRecordKeyWithSeq(key, nonTransactionSeqNum), 
        Type: data.LogRecordDeleted,
    }

    _, err := db.appendLogRecordWithLock(logRecord)
    if err != nil {
        return err
    }

    ok := db.index.Delete(key)
    if !ok {
        return ErrIndexUpdateFailed
    }

    return nil
}

func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
    db.mutex.RLock()
    defer db.mutex.RUnlock()

    iterator := db.index.Iterator(false)
    for iterator.Rewind(); iterator.Valid(); iterator.Next() {
        value, err := db.GetValueByPosition(iterator.Value())
        if err != nil {
            return err
        }

        if !fn(iterator.Key(), value) {
            break
        }
    }
    
    return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
    db.mutex.Lock()
    defer db.mutex.Unlock()

    return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
    if db.activeFile == nil {
        if err := db.setActiveDataFile(); err != nil {
            return nil, err
        }
    }

    encodedRecord, size := data.EncodedLogRecord(logRecord)
    if db.activeFile.WriteOffset + size > db.options.DataFileSize {
        if err := db.activeFile.Sync(); err != nil {
            return nil, err
        }

        db.olderFiles[db.activeFile.FileId] = db.activeFile

        if err := db.setActiveDataFile(); err != nil {
            return nil, err
        }
    }

    writeOffset := db.activeFile.WriteOffset
    if err := db.activeFile.Write(encodedRecord); err != nil {
        return nil, err
    }

    if db.options.SyncWrites {
        if err := db.activeFile.Sync(); err != nil {
            return nil, err
        }
    }

    pos := &data.LogRecordPos {
        FileId: db.activeFile.FileId,
        Offset: writeOffset,
    }

    return pos, nil
}

func (db *DB) setActiveDataFile() error {
    var initialFileId uint32 = 0

    if db.activeFile != nil {
        initialFileId = db.activeFile.FileId + 1
    }

    dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
    if err != nil {
        return err
    }

    db.activeFile = dataFile
    return nil
}

func (db *DB) loadDataFiles() error {
    dirEntries, err := os.ReadDir(db.options.DirPath)
    if err != nil {
        return err
    }

    var fileIds []uint32
    for _, entry := range dirEntries {
        if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
            splitNames := strings.Split(entry.Name(), ".")
            fileId, err := strconv.Atoi(splitNames[0])
            if err != nil {
                return ErrDataDirectoryCorrupted
            }
            fileIds = append(fileIds, uint32(fileId))
        }
    }

    sort.Slice(fileIds, func(i int, j int) bool {
        return fileIds[i] < fileIds[j]
    })
    db.fileIds = fileIds

    for i, fileId := range fileIds {
        dataFile, err := data.OpenDataFile(db.options.DirPath, fileId)
        if err != nil {
            return err
        }

        if i == len(fileIds) - 1 {
            db.activeFile = dataFile
        } else {
            db.olderFiles[fileId] = dataFile
        }
    }
    
    return nil
}

func (db *DB) loadIndexFromDataFiles() error {
    if len(db.fileIds) == 0 {
        return nil
    }

    updateIndex := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) {
        var ok bool
        if typ == data.LogRecordDeleted {
            ok = db.index.Delete(key)
        } else {
            ok = db.index.Put(key, logRecordPos)
        }

        if !ok {
            panic("Index update failed during startup")
        }
    }

    transactionRecords := make(map[uint64][]*data.TransactionRecord)
    var currentSeqNum = nonTransactionSeqNum
    
    for _, fileId := range db.fileIds {
        var dataFile *data.DataFile
        if fileId == db.activeFile.FileId {
            dataFile = db.activeFile
        } else {
            dataFile = db.olderFiles[fileId]
        }

        var offset int64 = 0
        for {
            logRecord, readLength, err := dataFile.ReadLogRecord(offset)
            if err != nil {
                if err == io.EOF {
                    break
                } else {
                    return err
                }
            }

            logRecordPos := &data.LogRecordPos {
                FileId: fileId,
                Offset: offset,
            }

            realKey, seqNum := parseLogRecordKeyWithSeq(logRecord.Key)
            if seqNum == nonTransactionSeqNum {
                updateIndex(realKey, logRecord.Type, logRecordPos)
            } else {
                if logRecord.Type == data.LogRecordTxFinished {
                    for _, transactionRecord := range transactionRecords[seqNum] {
                        updateIndex(transactionRecord.Record.Key, transactionRecord.Record.Type, transactionRecord.Pos)
                    }
                    delete(transactionRecords, seqNum)
                } else {
                    logRecord.Key = realKey
                    transactionRecords[seqNum] = append(transactionRecords[seqNum], &data.TransactionRecord {
                        Record: logRecord,
                        Pos: logRecordPos,
                    })
                }
            }

            if seqNum > currentSeqNum {
                currentSeqNum = seqNum
            }

            offset += readLength
        }

        if fileId == db.activeFile.FileId {
            db.activeFile.WriteOffset = offset
        }
    }

    db.seqNum = currentSeqNum

    return nil
}

func checkOptions(options Options) error {
    if options.DirPath == "" {
        return ErrDataDirectoryEmpty
    }

    if options.DataFileSize <= 0 {
        return ErrDataFileSizeInvalid
    }

    return nil
}
