package utils

import "testing"

func TestGetTestKey(t *testing.T) {
    key := GetTestKey(42)
    if string(key) != "kvdb-test-key-000000042" {
        t.Errorf("GetTestKey(42) = %s; want kvdb-test-key-000000042", key)
    }
}

func TestGetTestValue(t *testing.T) {
    value := GetTestValue(42)
    if len(value) != 42 + len("kvdb-test-value-") {
        t.Errorf("Length of GetTestValue(42) = %d; want %d", len(value), 42 + len("kvdb-test-value-"))
    }
}
