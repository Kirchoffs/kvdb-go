package data

import "kvdb-go/fio"

const DataFileSuffix = ".data"

type DataFile struct {
    FileId uint32
    WriteOffset int64
    IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
    return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
    return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
    return nil
}

func (df *DataFile) Sync() error {
    return nil
}
