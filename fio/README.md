# Notes

## Test
```
>> go test -v
```

If I run `go test -v file_io_manager_test.go`, then I get the error:
```
undefined: NewFileIOManager
command-line-arguments [build failed]
```

In short, `go test whatever_test.go` is not okay as that is not supported as help documented:
```
>> go help test
usage: go test [build/test flags] [packages] [build/test flags & test binary flags]
```

The minimum test unit is a package. So, I need to run `go test` in the package directory.
