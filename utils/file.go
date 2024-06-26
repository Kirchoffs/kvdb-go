package utils

import (
    "io/fs"
    "os"
    "path/filepath"
    "strings"
    "syscall"
)

func DirSize(dirPath string) (int64, error) {
    var size int64

    err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if !info.IsDir() {
            size += info.Size()
        }
        return nil
    })

    return size, err
}

func AvailableDiskSpace() (uint64, error) {
    wd, err := syscall.Getwd()
    if err != nil {
        return 0, err
    }

    var stat syscall.Statfs_t
    if err = syscall.Statfs(wd, &stat); err != nil {
        return 0, err
    }

    return stat.Bavail * uint64(stat.Bsize), nil
}

func CopyDir(src, dst string, exclude []string) error {
    if _, err := os.Stat(dst); os.IsNotExist(err) {
        if err := os.MkdirAll(dst, os.ModePerm); err != nil {
            return err
        }
    }

    return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
        fileName := strings.Replace(path, src, "", 1)
        if fileName == "" {
            return nil
        }

        for _, e := range exclude {
            matched, err := filepath.Match(e, fileName)
            if err != nil {
                return err
            }
            if matched {
                return nil
            }
        }

        if info.IsDir() {
            return os.MkdirAll(filepath.Join(dst, fileName), info.Mode())
        }

        data, err := os.ReadFile(filepath.Join(src, fileName))
        if err != nil {
            return err
        }
        return os.WriteFile(filepath.Join(dst, fileName), data, info.Mode())
    })
}
