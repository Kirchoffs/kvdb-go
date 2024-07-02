# Notes
## Golang
### Time
```
time.Now().Unix()       // (10 digits) 1719895730, max int32 is 2147483647
time.Now().UnixMilli()  // (13 digits) 1719895730694
time.Now().UnixMicro()  // (16 digits) 1719895730694605
time.Now().UnixNano()   // (19 digits) 1719895730694606700, if we used int64 to hold it, it will overflow in about 2262.
```
