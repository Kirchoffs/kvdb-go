package data

import (
    "fmt"
    "os"
    "path/filepath"
    "testing"

    "github.com/sirupsen/logrus"
    "github.com/stretchr/testify/assert"
)

func init() {
    logrus.SetOutput(os.Stdout)
    logrus.SetLevel(logrus.DebugLevel)
}

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

func TestDataFileRead(t *testing.T) {
    dirPath := os.TempDir()
    fileId := uint32(42)
    deleteFile(dirPath, fileId)

    dataFile, err := OpenDataFile(os.TempDir(), 42)
    assert.Nil(t, err)
    assert.NotNil(t, dataFile)

    recordAlpha := &LogRecord {
        Key: []byte("key"),
        Value: []byte("value"),
    }
    encodedRecordAlpha, encodedRecordAlphaSize := EncodedLogRecord(recordAlpha)
    err = dataFile.Write(encodedRecordAlpha)
    assert.Nil(t, err)
    readRecordAlpha, readRecordAlphaSize, err := dataFile.ReadLogRecord(0)
    assert.Nil(t, err)
    assert.Equal(t, recordAlpha, readRecordAlpha)
    assert.Equal(t, encodedRecordAlphaSize, readRecordAlphaSize)

    recordBeta := &LogRecord {
        Key: []byte("key-beta"),
        Value: []byte("value-beta"),
    }
    encodedRecordBeta, encodedRecordBetaSize := EncodedLogRecord(recordBeta)
    err = dataFile.Write(encodedRecordBeta)
    assert.Nil(t, err)
    readRecordBeta, readRecordBetaSize, err := dataFile.ReadLogRecord(encodedRecordAlphaSize)
    assert.Nil(t, err)
    assert.Equal(t, recordBeta, readRecordBeta)
    assert.Equal(t, encodedRecordBetaSize, readRecordBetaSize)

    recordDeleted := &LogRecord {
        Key: []byte("key-deleted"),
        Value: []byte("value-deleted"),
        Type: LogRecordDeleted,
    }
    encodedRecordDeleted, encodedRecordDeletedSize := EncodedLogRecord(recordDeleted)
    err = dataFile.Write(encodedRecordDeleted)
    assert.Nil(t, err)
    readRecordDeleted, readRecordDeletedSize, err := dataFile.ReadLogRecord(encodedRecordAlphaSize + encodedRecordBetaSize)
    assert.Nil(t, err)
    assert.Equal(t, recordDeleted, readRecordDeleted)
    assert.Equal(t, encodedRecordDeletedSize, readRecordDeletedSize)
}

func deleteFile(dirPath string, fileId uint32) {
    fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, DataFileSuffix))
    os.Remove(fileName)
}
