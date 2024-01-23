package data

import (
    "encoding/binary"
    "hash/crc32"
    log "github.com/sirupsen/logrus"
)

type LogRecordType = byte

const (
    LogRecordNormal LogRecordType = iota
    LogRecordDeleted LogRecordType = iota
    LogRecordTxFinished LogRecordType = iota
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

type TransactionRecord struct {
    Record *LogRecord
    Pos *LogRecordPos
}

// crc | type | key_size | value_size | key | value
//   4 |    1 |    max 5 |      max 5 | var |   var
func EncodedLogRecord(logRecord *LogRecord) ([]byte, int64) {
    header := make([]byte, maxLogRecordHeaderSize)

    header[4] = logRecord.Type
    var index = 5
    index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
    index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

    var size = index + len(logRecord.Key) + len(logRecord.Value)
    encodedBytes := make([]byte, size)
    copy(encodedBytes[:index], header[:index])
    copy(encodedBytes[index:], logRecord.Key)
    copy(encodedBytes[index + len(logRecord.Key):], logRecord.Value)

    crc := crc32.ChecksumIEEE(encodedBytes[4:])
    binary.LittleEndian.PutUint32(encodedBytes, crc)

    return encodedBytes, int64(size)
}

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
    if len(buf) <= 4 {
        log.Warn("Data file might be corrupted, header size is less than 4")
        return nil, 0
    }

    header := &LogRecordHeader{
        crc: binary.LittleEndian.Uint32(buf[:4]),
        recordType: buf[4],
    }

    var index = 5
    keySize, n := binary.Varint(buf[index:])
    header.keySize = uint32(keySize)
    index += n

    valueSize, n := binary.Varint(buf[index:])
    header.valueSize = uint32(valueSize)
    index += n

    return header, int64(index)
}

func GetLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
    if logRecord == nil {
        return 0
    }

    crc := crc32.ChecksumIEEE(header[:])
    crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
    crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

    return crc
}
