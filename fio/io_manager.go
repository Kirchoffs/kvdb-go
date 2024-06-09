package fio

const DataFilePerm = 0644

type IOType = byte
const (
    StandardFileIO IOType = iota
    MemoryMapIO
)

type IOManager interface {
    Read([]byte, int64) (int, error)
    Write([]byte) (int, error)
    Sync() error
    Close() error
    Size() (int64, error)
}

func NewIOManager(fileName string, ioType IOType) (IOManager, error) {
    switch ioType {
    case StandardFileIO:
        return NewFileIOManager(fileName)
    case MemoryMapIO:
        return NewMMapIOManager(fileName)
    default:
        panic("Unknown IO type")
    }
}
