package data

import (
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "kvdb-go/fio"
    "path/filepath"
    log "github.com/sirupsen/logrus"
)

var (
    ErrInvalidCRC = errors.New("invalid crc value, log record may be corrupted")
    ErrDataFileCorrupted = errors.New("data file is corrupted")
)

const (
    LogRecordEntryFormatString = "offset: %d, crc: %d, header.crc: %d, header.recordType: %d, header.keySize: %d, header.valueSize: %d, key: %s, value: %s"
)

const DataFileSuffix = ".data"

type DataFile struct {
    FileId uint32
    WriteOffset int64
    IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
    fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, DataFileSuffix))
    
    ioManager, err := fio.NewIOManager(fileName)
    if err != nil {
        return nil, err
    }

    return &DataFile{fileId, 0, ioManager}, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
    fileSize, err := df.IOManager.Size()
    if err != nil {
        return nil, 0, err
    }
    if offset == fileSize {
        return nil, 0, io.EOF
    } else if offset > fileSize {
        log.Warn("Data file might be corrupted, offset is larger than file size")
        return nil, 0, io.EOF
    }

    var readHeaderSize int64 = maxLogRecordHeaderSize
    if offset + readHeaderSize > fileSize {
        readHeaderSize = fileSize - offset
    }

    headerBuffer, err := df.readNBytes(readHeaderSize, offset)
    if err != nil {
        return nil, 0, err
    }

    header, headerSize := DecodeLogRecordHeader(headerBuffer)
    if header == nil {
        return nil, 0, nil
    }
    if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
        log.Warn("Data file might be corrupted, there are some extra zero value bytes in the end of file")
        return nil, 0, io.EOF
    }

    keySize, valueSize := int64(header.keySize), int64(header.valueSize)
    if (keySize <= 0) {
        log.Error("Data file is corrupted, key size is less than or equal to zero")
        return nil, 0, ErrDataFileCorrupted
    }
    var recordSize = headerSize + keySize + valueSize

    logRecord := &LogRecord{Type: header.recordType}
    kvBuffer, err := df.readNBytes(keySize + valueSize, offset + headerSize)
    if err != nil {
        return nil, 0, err
    }

    logRecord.Key = kvBuffer[:keySize]
    logRecord.Value = kvBuffer[keySize:]

    crc := GetLogRecordCRC(logRecord, headerBuffer[crc32.Size : headerSize])
    
    if crc != header.crc {
        log.Error("Data file is corrupted, crc value is not matched")
        log.Error(fmt.Sprintf(
            LogRecordEntryFormatString, 
            offset, crc, header.crc, header.recordType, header.keySize, header.valueSize, logRecord.Key, logRecord.Value,
        ))
        return nil, 0, ErrInvalidCRC
    }

    log.Debug(fmt.Sprintf(
        LogRecordEntryFormatString, 
        offset, crc, header.crc, header.recordType, header.keySize, header.valueSize, logRecord.Key, logRecord.Value,
    ))
    
    return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
    n, err := df.IOManager.Write(buf)
    if err != nil {
        return err
    }
    df.WriteOffset += int64(n)
    return nil
}

func (df *DataFile) Sync() error {
    return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
    return df.IOManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (buffer []byte, err error) {
    buffer = make([]byte, n)
    _, err = df.IOManager.Read(buffer, offset)
    return
}
