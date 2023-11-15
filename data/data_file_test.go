package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
    dataFile1, err := OpenDataFile(os.TempDir(), 0)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile1)

    dataFile2, err := OpenDataFile(os.TempDir(), 42)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile2)

    dataFile3, err := OpenDataFile(os.TempDir(), 0)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile3)
}

func TestDataFileWrite(t *testing.T) {
    dataFile, err := OpenDataFile(os.TempDir(), 0)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile)

    err = dataFile.Write([]byte("Hello World!"))
    assert.Nil(t, err)
    
    err = dataFile.Write([]byte("Konnichiwa Sekai!"))
    assert.Nil(t, err)
}

func TestDataFileClose(t *testing.T) {
    dataFile, err := OpenDataFile(os.TempDir(), 42)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile)

    err = dataFile.Close()
    assert.Nil(t, err)
}

func TestDataFileSync(t *testing.T) {
    dataFile, err := OpenDataFile(os.TempDir(), 42)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile)

    err = dataFile.Write([]byte("Hello World!"))
    assert.Nil(t, err)

    err = dataFile.Sync()
    assert.Nil(t, err)
}
