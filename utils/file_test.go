package utils

import (
    "os"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
    dir, _ := os.Getwd()

    dirSize, err := DirSize(dir)
    assert.Nil(t, err)
    assert.True(t, dirSize > 0)
}

func TestAvailableDiskSpace(t *testing.T) {
    space, err := AvailableDiskSpace()
    assert.Nil(t, err)
    assert.True(t, space > 0)
}
