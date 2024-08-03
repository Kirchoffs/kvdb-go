package kvdb_go

import (
    "io"
    "kvdb-go/data"
    "kvdb-go/utils"
    "os"
    "path"
    "path/filepath"
    "sort"
    "strconv"
)

const (
    mergeDirName     = "-merge"
    mergeFinishedKey = "merge-finished"
)

func (db *DB) Merge() error {
    if db.activeFile == nil {
        return nil
    }

    db.mutex.Lock()
    if db.isMerging {
        db.mutex.Unlock()
        return ErrMergeInProgress
    }

    // Check if the amount of data can be merged is greater than the merge trigger ratio
    totalSize, err := utils.DirSize(db.options.DirPath)
    if err != nil {
        db.mutex.Unlock()
        return err
    }
    if float32(db.reclaimableSpace)/float32(totalSize) < db.options.MergeTriggerRatio {
        db.mutex.Unlock()
        return ErrMergeTriggerRatioNotReached
    }

    // Check if the available disk space is enough for merge
    // When merging, there are old data (totalSize) and merged data (totalSize - reclaimableSpace)
    availableDiskSize, err := utils.AvailableDiskSpace()
    if err != nil {
        db.mutex.Unlock()
        return err
    }
    if uint64(totalSize-db.reclaimableSpace) >= availableDiskSize {
        db.mutex.Unlock()
        return ErrDiskSpaceNotEnoughForMerge
    }

    db.isMerging = true
    defer func() {
        db.isMerging = false
    }()

    if err := db.activeFile.Sync(); err != nil {
        db.mutex.Unlock()
        return err
    }

    db.olderFiles[db.activeFile.FileId] = db.activeFile
    if err := db.setActiveDataFile(); err != nil {
        db.mutex.Unlock()
        return nil
    }

    nonMergeFileId := db.activeFile.FileId

    var mergeFiles []*data.DataFile
    for _, file := range db.olderFiles {
        mergeFiles = append(mergeFiles, file)
    }
    db.mutex.Unlock()

    sort.Slice(mergeFiles, func(i, j int) bool {
        return mergeFiles[i].FileId < mergeFiles[j].FileId
    })

    mergePath := db.getMergePath()
    if _, err := os.Stat(mergePath); os.IsNotExist(err) {
        if err := os.RemoveAll(mergePath); err != nil {
            return err
        }
    }

    if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
        return err
    }

    mergeOptions := db.options
    mergeOptions.DirPath = mergePath
    mergeOptions.SyncWrites = false // Batch write
    mergeDB, err := Open(mergeOptions)
    if err != nil {
        return err
    }

    hintFile, err := data.OpenHintFile(mergePath)
    if err != nil {
        return err
    }

    for _, dataFile := range mergeFiles {
        var offset int64 = 0
        for {
            logRecord, size, err := dataFile.ReadLogRecord(offset)
            if err != nil {
                if err == io.EOF {
                    break
                }
                return err
            }

            realKey, _ := parseLogRecordKeyWithSeq(logRecord.Key)
            logRecordPos := db.index.Get(realKey)
            // Check if it is a valid data
            if logRecordPos != nil && logRecordPos.FileId == dataFile.FileId && logRecordPos.Offset == offset {
                logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNum)
                mergeLogRecordPos, err := mergeDB.appendLogRecord(logRecord)
                if err != nil {
                    return err
                }

                if err := hintFile.WriteHintRecord(realKey, mergeLogRecordPos); err != nil {
                    return err
                }
            }

            offset += size
        }
    }

    if err := hintFile.Sync(); err != nil {
        return err
    }

    if err := mergeDB.Sync(); err != nil {
        return err
    }

    mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
    if err != nil {
        return err
    }
    mergeFinishedRecord := &data.LogRecord{
        Key:   []byte(mergeFinishedKey),
        Value: []byte(strconv.Itoa(int(nonMergeFileId))),
    }
    encodedRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
    if err := mergeFinishedFile.Write(encodedRecord); err != nil {
        return err
    }
    if err := mergeFinishedFile.Sync(); err != nil {
        return err
    }

    return nil
}

// From /path/kvdb to /path/kvdb-merge
func (db *DB) getMergePath() string {
    dir := path.Dir(db.options.DirPath)   // path
    base := path.Base(db.options.DirPath) // kvdb

    return path.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
    mergePath := db.getMergePath()

    if _, err := os.Stat(mergePath); os.IsNotExist(err) {
        return nil
    }
    defer func() {
        _ = os.RemoveAll(mergePath)
    }()

    dirEntries, err := os.ReadDir(mergePath)
    if err != nil {
        return err
    }

    var mergeFinished bool
    var mergeFileNames []string
    for _, entry := range dirEntries {
        if entry.Name() == mergeFinishedKey {
            mergeFinished = true
        }
        if entry.Name() == data.SeqNumFileName {
            continue
        }
        if entry.Name() == fileLockName {
            continue
        }
        mergeFileNames = append(mergeFileNames, entry.Name())
    }

    if !mergeFinished {
        return nil
    }

    nonMergeFileId, err := db.getNonMergeFileId(mergePath)
    if err != nil {
        return err
    }

    var fileId uint32
    for ; fileId < nonMergeFileId; fileId++ {
        fileName := data.GetDataFileName(db.options.DirPath, fileId)
        if _, err := os.Stat(fileName); err == nil {
            if err := os.Remove(fileName); err != nil {
                return err
            }
        }
    }

    // Move from /path/kvdb-merge/1.data to /path/kvdb/1.data
    for _, fileName := range mergeFileNames {
        srcPath := filepath.Join(mergePath, fileName)
        dstPath := filepath.Join(db.options.DirPath, fileName)

        if err := os.Rename(srcPath, dstPath); err != nil {
            return err
        }
    }

    return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
    mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
    if err != nil {
        return 0, err
    }

    record, _, err := mergeFinishedFile.ReadLogRecord(0)
    if err != nil {
        return 0, err
    }

    nonMergeFileId, err := strconv.Atoi(string(record.Value))
    if err != nil {
        return 0, err
    }

    return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
    hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
    if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
        return nil
    }

    hintFile, err := data.OpenHintFile(db.options.DirPath)
    if err != nil {
        return err
    }

    var offset int64 = 0
    for {
        logRecord, size, err := hintFile.ReadLogRecord(offset)
        if err != nil {
            if err == io.EOF {
                break
            }
            return err
        }

        pos := data.DecodeLogRecordPos(logRecord.Value)
        db.index.Put(logRecord.Key, pos)
        offset += size
    }

    return nil
}
