package fio

import (
    "os"
    "golang.org/x/exp/mmap"
)

type MMap struct {
    readerAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
    _, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
    if err != nil {
        return nil, err
    }

    readerAt, err := mmap.Open(filename)
    if err != nil {
        return nil, err
    }
    return &MMap{readerAt}, nil
}

func (mmap *MMap) Read(p []byte, off int64) (int, error) {
    return mmap.readerAt.ReadAt(p, off)
}

func (mmap *MMap) Write([]byte) (int, error) {
    panic("not implemented")
}

func (mmap *MMap) Sync() error {
    panic("not implemented")
}

func (mmap *MMap) Close() error {
    return mmap.readerAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
    return int64(mmap.readerAt.Len()), nil
}
