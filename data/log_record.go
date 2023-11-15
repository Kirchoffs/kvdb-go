package data

import "encoding/binary"

type LogRecordType = byte

const (
    LogRecordNormal LogRecordType = iota
    LogRecordDeleted LogRecordType = iota
)

// crc type key_size value_size key value
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32 + binary.MaxVarintLen32

type LogRecord struct {
    Key []byte
    Value []byte
    Type LogRecordType
}

type LogRecordHeader struct {
    crc uint32
    recordType LogRecordType
    keySize uint32
    valueSize uint32
}

type LogRecordPos struct {
    FileId uint32
    Offset int64
}

func EncodedLogRecord(logRecord *LogRecord) ([]byte, int64) {
    return nil, 0
}

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
    return nil, 0
}

func GetLogRecordCRC(lr *LogRecord, header []byte) uint32 {
    return 0
}
