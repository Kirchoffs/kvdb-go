# Notes

## Test
```
>> go test -v
```

If I run `go test -v file_io_test.go`, then I get the error:
```
undefined: NewFileIOManager
command-line-arguments [build failed]
```

In short, `go test whatever_test.go` is not okay as that is not supported as help documented.