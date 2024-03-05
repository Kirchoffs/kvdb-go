package data

import (
    "hash/crc32"
    "os"
    "testing"

    "github.com/sirupsen/logrus"
    "github.com/stretchr/testify/assert"
)

func init() {
    logrus.SetOutput(os.Stdout)
    logrus.SetLevel(logrus.DebugLevel)
}

func TestEncodeLogRecord(t *testing.T) {
    logRecord := &LogRecord{
        []byte("key"),
        []byte("value"),
        LogRecordNormal,
    }
    encodedBytes, encodedBytesLength := EncodeLogRecord(logRecord)
    assert.NotNil(t, encodedBytes)
    assert.Equal(t, encodedBytesLength, int64(4 + 1 + 1 + 1 + 3 + 5))
    t.Log(encodedBytes)
    t.Log(encodedBytesLength)

    logRecordEmptyValue := &LogRecord{
        Key: []byte("key"),
        Type: LogRecordNormal,
    }
    encodedBytes, encodedBytesLength = EncodeLogRecord(logRecordEmptyValue)
    assert.NotNil(t, encodedBytes)
    assert.Equal(t, encodedBytesLength, int64(4 + 1 + 1 + 1 + 3 + 0))
    t.Log(encodedBytes)
    t.Log(encodedBytesLength)

    logRecordDeleted := &LogRecord{
        []byte("key"),
        []byte("value"),
        LogRecordDeleted,
    }
    encodedBytes, encodedBytesLength = EncodeLogRecord(logRecordDeleted)
    assert.NotNil(t, encodedBytes)
    assert.Equal(t, encodedBytesLength, int64(4 + 1 + 1 + 1 + 3 + 5))
    t.Log(encodedBytes)
    t.Log(encodedBytesLength)
}

func TestDecodeLogRecordHeader(t *testing.T) {
    logRecord := &LogRecord{
        []byte("key"),
        []byte("value"),
        LogRecordNormal,
    }
    encodedBytes, _ := EncodeLogRecord(logRecord)
    header, headerSize := DecodeLogRecordHeader(encodedBytes)
    assert.NotNil(t, header)
    assert.Equal(t, headerSize, int64(4 + 1 + 1 + 1))
    assert.Equal(t, header.recordType, LogRecordNormal)
    assert.Equal(t, header.keySize, uint32(3))
    assert.Equal(t, header.valueSize, uint32(5))

    logRecordEmptyValue := &LogRecord{
        Key: []byte("key"),
        Type: LogRecordNormal,
    }
    encodedBytes, _ = EncodeLogRecord(logRecordEmptyValue)
    header, headerSize = DecodeLogRecordHeader(encodedBytes)
    assert.NotNil(t, header)
    assert.Equal(t, headerSize, int64(4 + 1 + 1 + 1))
    assert.Equal(t, header.recordType, LogRecordNormal)
    assert.Equal(t, header.keySize, uint32(3))
    assert.Equal(t, header.valueSize, uint32(0))

    logRecordDeleted := &LogRecord{
        []byte("key"),
        []byte("value"),
        LogRecordDeleted,
    }
    encodedBytes, _ = EncodeLogRecord(logRecordDeleted)
    header, headerSize = DecodeLogRecordHeader(encodedBytes)
    assert.NotNil(t, header)
    assert.Equal(t, headerSize, int64(4 + 1 + 1 + 1))
    assert.Equal(t, header.recordType, LogRecordDeleted)
    assert.Equal(t, header.keySize, uint32(3))
    assert.Equal(t, header.valueSize, uint32(5))
}

func TestGetLogRecordCRC(t *testing.T) {
    logRecord := &LogRecord{
        []byte("key"),
        []byte("value"),
        LogRecordNormal,
    }
    encodedBytes, _ := EncodeLogRecord(logRecord)
    header, _ := DecodeLogRecordHeader(encodedBytes)
    crc := GetLogRecordCRC(logRecord, encodedBytes[crc32.Size : crc32.Size + 1 + 1 + 1])
    assert.Equal(t, crc, header.crc)

    logRecordEmptyValue := &LogRecord{
        Key: []byte("key"),
        Type: LogRecordNormal,
    }
    encodedBytes, _ = EncodeLogRecord(logRecordEmptyValue)
    header, _ = DecodeLogRecordHeader(encodedBytes)
    crc = GetLogRecordCRC(logRecordEmptyValue, encodedBytes[crc32.Size : crc32.Size + 1 + 1 + 1])
    assert.Equal(t, crc, header.crc)

    logRecordDeleted := &LogRecord{
        []byte("key"),
        []byte("value"),
        LogRecordDeleted,
    }
    encodedBytes, _ = EncodeLogRecord(logRecordDeleted)
    header, _ = DecodeLogRecordHeader(encodedBytes)
    crc = GetLogRecordCRC(logRecordDeleted, encodedBytes[crc32.Size : crc32.Size + 1 + 1 + 1])
    assert.Equal(t, crc, header.crc)
}
