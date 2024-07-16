# Notes
## Golang
### Time
```
time.Now().Unix()       // (10 digits) 1719895730, max int32 is 2147483647
time.Now().UnixMilli()  // (13 digits) 1719895730694
time.Now().UnixMicro()  // (16 digits) 1719895730694605
time.Now().UnixNano()   // (19 digits) 1719895730694606700, if we used int64 to hold it, it will overflow in about 2262.
```

## RESP
Redis Serialization Protocol

### String
Start with a "+" character, followed by a string terminated by "\r\n".
```
+OK\r\n
```

### Error
Start with a "-" character, followed by an error message terminated by "\r\n".
```
-ERR unknown command 'foobar'\r\n
```

### Integer
Start with a ":" character, followed by an integer terminated by "\r\n".
```
:42\r\n
```

### Bulk String
Start with a "$" character, followed by the number of bytes in the string terminated by "\r\n", then the string itself terminated by "\r\n".
```
$6\r\nfoobar\r\n
```

### Array
Start with a "*" character, followed by the number of elements in the array terminated by "\r\n", then the elements themselves.
```
*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```

## Run
```
>> cd cmd
>> go build
>> ./cmd
```

```
>> ./redis-cli -p 6380
```
