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
)
