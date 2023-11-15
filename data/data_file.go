package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kvdb-go/fio"
	"path/filepath"
)

var (
    ErrInvalidCRC = errors.New("invalid crc value, log record may be corrupted")
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

    var headerBytes int64 = maxLogRecordHeaderSize
    if offset + headerBytes > fileSize {
        headerBytes = fileSize - offset
    }

    headerBuffer, err := df.readNBytes(headerBytes, offset)
    if err != nil {
        return nil, 0, err
    }

    header, headerSize := DecodeLogRecordHeader(headerBuffer)
    if header == nil {
        return nil, 0, nil
    }
    if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
        return nil, 0, io.EOF
    }

    keySize, valueSize := int64(header.keySize), int64(header.valueSize)
    var recordSize = headerSize + keySize + valueSize

    logRecord := &LogRecord{Type: header.recordType}
    if keySize >= 0 || valueSize >= 0 {
        kvBuffer, err := df.readNBytes(keySize + valueSize, offset + headerSize)
        if err != nil {
            return nil, 0, err
        }

        logRecord.Key = kvBuffer[:keySize]
        logRecord.Value = kvBuffer[keySize:]
    }

    crc := GetLogRecordCRC(logRecord, headerBuffer[crc32.Size : headerSize])
    if crc != header.crc {
        return nil, 0, ErrInvalidCRC
    }
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
