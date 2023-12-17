package kvdb_go

import "os"

type Options struct {
    DirPath string
    DataFileSize int64
    SyncWrites bool
    IndexType IndexType
}

type IndexType = int8

const (
    BTreeIndex IndexType = iota + 1
    ARTIndex
)

var DefaultOptions = Options{
    DirPath: os.TempDir(),
    DataFileSize: 1 << 28,
    SyncWrites: false,
    IndexType: BTreeIndex,
}
