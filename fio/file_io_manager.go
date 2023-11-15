package fio

import "os"

type FileIOManager struct {
    fd *os.File
}

func NewFileIOManager(fileName string) (*FileIOManager, error) {
    fd, err := os.OpenFile(fileName, os.O_CREATE | os.O_RDWR | os.O_APPEND, DataFilePerm)
    if err != nil {
        return nil, err
    }
    return &FileIOManager{fd}, nil
}

func (fio *FileIOManager) Read(b []byte, offset int64) (int, error) {
    return fio.fd.ReadAt(b, offset)
}

func (fio *FileIOManager) Write(b []byte) (int, error) {
    return fio.fd.Write(b)
}

func (fio *FileIOManager) Sync() error {
    return fio.fd.Sync()
}

func (fio *FileIOManager) Close() error {
    return fio.fd.Close()
}

func (fio *FileIOManager) Size() (int64, error) {
    stat, err := fio.fd.Stat()
    if err != nil {
        return 0, err
    }
    return stat.Size(), nil
}
