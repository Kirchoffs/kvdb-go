package kvdb_go

import "os"

type Options struct {
    DirPath string
    DataFileSize int64
    SyncWrites bool
    BytesPerSync uint
    IndexType IndexType
    MMapAtStart bool
}

type IteratorOptions struct {
    Prefix []byte
    Reverse bool
}

type WriteBatchOptions struct {
    MaxBatchSize uint
    SyncWrites bool
}

type IndexType = int8

const (
    BTreeIndex IndexType = iota + 1
    ARTIndex
    BPTreeIndex
)

var DefaultOptions = Options {
    DirPath: os.TempDir(),
    DataFileSize: 1 << 28,
    SyncWrites: false,
    BytesPerSync: 0,
    IndexType: BTreeIndex,
    MMapAtStart: true,
}

var DefaultIteratorOptions = IteratorOptions {
    Prefix: nil,
    Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions {
    MaxBatchSize: 10000,
    SyncWrites: true,
}
