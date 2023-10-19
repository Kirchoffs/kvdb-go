package data

type LogRecordType = byte

const (
    LogRecordNormal LogRecordType = iota
    LogRecordDeleted LogRecordType = iota
)

type LogRecord struct {
    Key []byte
    Value []byte
    Type LogRecordType
}

type LogRecordPos struct {
    Fid    uint32
    Offset int64
}

func EncodedLogRecord(logRecord *LogRecord) ([]byte, int64) {
    return nil, 0
}
