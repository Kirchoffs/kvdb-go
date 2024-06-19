package kvdb_go

import (
    "encoding/binary"
    "kvdb-go/data"
    "sync"
    "sync/atomic"
)

const nonTransactionSeqNum uint64 = 0
var txFinishedKey = []byte("tx-finished")

type WriteBatch struct {
    options WriteBatchOptions
    mutex *sync.Mutex
    db *DB
    pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
    if db.options.IndexType == BPTreeIndex && !db.seqNumFileExists && !db.isFirstLaunch {
        panic("cannot use WriteBatch with BPTreeIndex without sequence number")
    }

    return &WriteBatch {
        options: options,
        mutex: new(sync.Mutex),
        db: db,
        pendingWrites: make(map[string]*data.LogRecord),
    }
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
    if len(key) == 0 {
        return ErrKeyIsEmpty
    }

    wb.mutex.Lock()
    defer wb.mutex.Unlock()

    logRecord := &data.LogRecord{Key: key, Value: value}
    wb.pendingWrites[string(key)] = logRecord
    return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
    if len(key) == 0 {
        return ErrKeyIsEmpty
    }

    wb.mutex.Lock()
    defer wb.mutex.Unlock()

    logRecordPos := wb.db.index.Get(key)
    if logRecordPos == nil {
        if wb.pendingWrites[string(key)] != nil {
            delete(wb.pendingWrites, string(key))
        }
        return nil
    }

    logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
    wb.pendingWrites[string(key)] = logRecord
    return nil
}

func (wb *WriteBatch) Commit() error {
    wb.mutex.Lock()
    defer wb.mutex.Unlock()

    if len(wb.pendingWrites) == 0 {
        return nil
    }
    if uint(len(wb.pendingWrites)) > wb.options.MaxBatchSize {
        return ErrExceedMaxBatchSize
    }

    wb.db.mutex.Lock()
    defer wb.db.mutex.Unlock()

    // Here wb.db.seqNum is updated atomically
    seqNum := atomic.AddUint64(&wb.db.seqNum, 1)

    positions := make(map[string]*data.LogRecordPos)
    for _, record := range wb.pendingWrites {
        logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord {
            Key: logRecordKeyWithSeq(record.Key, seqNum),
            Value: record.Value,
            Type: record.Type,
        })

        if err != nil {
            return err
        }

        positions[string(record.Key)] = logRecordPos
    }

    finishedRecord := &data.LogRecord {
        Key: logRecordKeyWithSeq(txFinishedKey, seqNum),
        Type: data.LogRecordTxFinished,
    }

    if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
        return err
    }

    if wb.options.SyncWrites && wb.db.activeFile != nil {
        if err := wb.db.activeFile.Sync(); err != nil {
            return err
        }
    }

    for _, record := range wb.pendingWrites {
        pos := positions[string(record.Key)]
        var oldPos *data.LogRecordPos

        if record.Type == data.LogRecordDeleted {
            oldPos, _ = wb.db.index.Delete(record.Key)
        } else if record.Type == data.LogRecordNormal {
            oldPos = wb.db.index.Put(record.Key, pos)
        }

        if oldPos != nil {
            wb.db.reclaimableSpace += int64(oldPos.Size)
        }
    }

    wb.pendingWrites = make(map[string]*data.LogRecord)
    
    return nil
}

func logRecordKeyWithSeq(key []byte, seqNum uint64) []byte {
    seq := make([]byte, binary.MaxVarintLen64)
    n := binary.PutUvarint(seq, seqNum)

    encodingKey := make([]byte, n + len(key))
    copy(encodingKey[:n], seq)
    copy(encodingKey[n:], key)

    return encodingKey
}

func parseLogRecordKeyWithSeq(key []byte) ([]byte, uint64) {
    seqNum, n := binary.Uvarint(key)
    return key[n:], seqNum
}
