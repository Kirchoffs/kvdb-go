package utils

import "strconv"

func Float64ToByte(f float64) []byte {
    return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}

func ByteToFloat64(b []byte) float64 {
    res, _ := strconv.ParseFloat(string(b), 64)
    return res
}
