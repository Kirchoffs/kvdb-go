package kvdb_go

import "errors"

var (
    ErrKeyIsEmpty = errors.New("key is empty")
    ErrIndexUpdateFailed = errors.New("index update failed")
    ErrKeyNotFound = errors.New("key not found")
    ErrDataFileNotFound = errors.New("data file not found")
    ErrDataDirectoryEmpty = errors.New("data directory path is empty")
    ErrDataFileSizeInvalid = errors.New("data file size is invalid")
    ErrDataDirectoryCorrupted = errors.New("data directory is corrupted")
    ErrExceedMaxBatchSize = errors.New("exceed max batch size")
    ErrMergeInProgress = errors.New("merge in progress")
    ErrDatabaseIsInUse = errors.New("database is in use")
    ErrMergeTriggerRatioInvalid = errors.New("merge trigger ratio is invalid")
    ErrMergeTriggerRatioNotReached = errors.New("merge trigger ratio not reached")
    ErrDiskSpaceNotEnoughForMerge = errors.New("disk space not enough for merge")
)
