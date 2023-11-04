package kvdb_go

type Options struct {
    DirPath string
    DataFileSize int64
    SyncWrite bool
    IndexType IndexType
}

type IndexType = int8

const (
    BTreeIndex IndexType = iota + 1
    ARTIndex
)
