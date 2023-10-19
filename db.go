package kvdb_go

import (
	"kvdb-go/data"
	"kvdb-go/index"
	"sync"
)

type DB struct {
    options *Options
    mutex *sync.RWMutex
    activeFile *data.DataFile
    olderFiles map[uint32]*data.DataFile
    index index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
    if len(key) == 0 {
        return ErrKeyIsEmpty
    }

    logRecord := &data.LogRecord {
        Key: key,
        Value: value,
        Type: data.LogRecordNormal,
    }

    if pos, err := db.appendLogRecord(logRecord); err != nil {
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

    var dataFile *data.DataFile
    if db.activeFile.FileId == logRecordPos.Fid {
        dataFile = db.activeFile
    } else {
        dataFile = db.olderFiles[logRecordPos.Fid]
    }

    if dataFile == nil {
        return nil, ErrDataFileNotFound
    }

    logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
    if err != nil {
        return nil, nil
    }

    if logRecord.Type == data.LogRecordDeleted {
        return nil, ErrKeyNotFound
    }

    return logRecord.Value, nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
    db.mutex.Lock()
    defer db.mutex.Unlock()

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

    if db.options.SyncWrite {
        if err := db.activeFile.Sync(); err != nil {
            return nil, err
        }
    }

    pos := &data.LogRecordPos {
        Fid: db.activeFile.FileId,
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
