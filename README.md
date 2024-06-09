# Notes

## Get Started
```
>> go mod init kvdb-go
>> go get github.com/google/btree
>> go get github.com/stretchr/testify
>> go get github.com/plar/go-adaptive-radix-tree
>> go get github.com/boltdb/bolt
>> go get github.com/gofrs/flock
>> go get golang.org/x/exp/mmap
```

```
>> go mod tidy
```

## Test
Run tests:
```
>> go test ./...
>> go test ./... -v
>> go test -timeout 30s -run ^TestOpenDataFile$ kvdb-go/data
>> go test -timeout 30s -run ^TestOpenDataFile$ kvdb-go/data -v
```

Run examples:
```
>> cd examples
>> go run basic_operations.go
```

## Tools
### View raw binary data
```
>> hexdump -C <filename>
```

## Project Notes
### index/index.go
```
type Item struct {
    key []byte
    pos *data.LogRecordPos
}

func (x *Item) Less(y btree.Item) bool {
    return bytes.Compare(x.key, y.(*Item).key) == -1
}
```

Here the btree.Item is like:
```
type Item interface {
    Less(than Item) bool
}
```

### FileIOManager
FileIOManager is a struct that manages the file IO operations. It is used to read and write data to the file. It does not know the data format.

DataFile will use FileIOManager to read and write data.

### DataFile
```
type DataFile struct {
    FileId uint32
    WriteOffset int64
    IOManager fio.IOManager
}
```

### Data
For this DB, key cannot be empty, value can be empty (intuitive way).

## Golang Notes
### RWMutex
```
package main

import (
	"fmt"
	"sync"
	"time"
)

var (
    rwLock sync.RWMutex
)

func main() {
    readAndWrite()
}

func readAndWrite() {
    go read()
    go read()
    go read()
    go write()

    time.Sleep(5 * time.Second)
    fmt.Println("Done")
}

func read() {
    rwLock.RLock()
    defer rwLock.RUnlock()

    fmt.Println("Read locking")
    time.Sleep(time.Second)
    fmt.Println("Read unlocking")
}

func write() {
    rwLock.Lock()
    defer rwLock.Unlock()

    fmt.Println("Write locking")
    time.Sleep(time.Second)
    fmt.Println("Write unlocking")
}
```

### Type Assertions
A type assertion provides access to an interface value's underlying concrete value.
```
t := i.(T)
```
This statement asserts that the interface value i holds the concrete type T and assigns the underlying T value to the variable t. __If i does not hold a T, the statement will trigger a panic.__

To test whether an interface value holds a specific type, a type assertion can return two values: the underlying value and a boolean value that reports whether the assertion succeeded.
```
t, ok := i.(T)
```
If i does not hold a T, ok will be false and t will be the zero value of type T, and no panic occurs.

```
package main

import "fmt"

func main() {
    var i interface{} = "hello"

    s := i.(string)
    fmt.Println(s)         // hello

    s, ok := i.(string)
    fmt.Println(s, ok)     // hello, true

    f, ok := i.(float64)
    fmt.Println(f, ok)     // 0, false

    f = i.(float64)        // panic
    fmt.Println(f)
}
```

```
package main

import "fmt"

type Person struct {
    Name string
    Age  int
}

func main() {
    values := []interface{}{
        42,
        "hello",
        &Person{Name: "Alice", Age: 30},
    }

    for _, v := range values {
        if p, ok := v.(*Person); ok {
            fmt.Printf("Name: %s, Age: %d\n", p.Name, p.Age)
        } else {
            fmt.Println("Not a person")
        }
    }
}
```

```
package main

import "fmt"

type Person struct {
    Name string
    Age  int
}

func main() {
    values := []interface{}{
        42,
        "hello",
        Person{Name: "Alice", Age: 30},
    }

    for _, v := range values {
        if p, ok := v.(Person); ok {
            fmt.Printf("Name: %s, Age: %d\n", p.Name, p.Age)
        } else {
            fmt.Println("Not a person")
        }
    }
}
```

### Duck Typing
In btree_generic.go
```
type Item interface {
	// Less tests whether the current item is less than the given argument.
	//
	// This must provide a strict weak ordering.
	// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
	// hold one of either a or b in the tree).
	Less(than Item) bool
}
```

In index/index.go
```
type Item struct {
    key []byte
    pos *data.LogRecordPos
}

func (x *Item) Less(y btree.Item) bool {
    return bytes.Compare(x.key, y.(*Item).key) == -1
}
```
Here, `*Item` implemnts the `Less` method of `btree.Item`.

Why does the below code not work?
```
func (x *Item) Less(y btree.Item) bool {
    return bytes.Compare(x.key, y.(Item).key) == -1
}
```
Because `Item` does not implement `btree.Item`, so `Item` cannot be regarded as `btree.Item`. Only `*Item` implements the `btree.Item`.

### Package strconv
Package strconv package provides functions for converting strings to other types and vice versa. The name "strconv" stands for "string conversion."

```
import "strconv"

func main() {
    i, err := strconv.Atoi("-42")
    s := strconv.Itoa(-42)
    fmt.Println(i, s)   // -42 -42
}
```

### Blank Identifier
```
package main

import "fmt"

func main() {
    x := []int{1, 2, 3}
    y := []int{1, 2, 3}

    for _, ex := range x {
        for _, ey := range y {
            fmt.Println(ex, ey)
        }
    }
}
```

### Enum
```
type IndexType = int8

const (
    BTreeIndex IndexType = iota + 1
    ARTIndex
)
```
BTreeIndex is 1, ARTIndex is 2.

```
const (
    LogRecordNormal LogRecordType = iota
    LogRecordDeleted LogRecordType = iota
)
```
LogRecordNormal is 0, LogRecordDeleted is 1.

### Conversion
#### Convert []byte to string
```
import "fmt"

world := []byte{'w', 'o', 'r', 'l', 'd'}
fmt.Println(world)
fmt.Println(string(world))
```

#### Convert string to []byte
```
import "fmt"

hello := "hello"
fmt.Println(hello)
fmt.Println([]byte(hello))
```

### Shuffle
```
var randomList []int
for i := 1; i < 999; i++ {
    randomList = append(randomList, i)
}
source := rand.NewSource(time.Now().UnixNano())
random := rand.New(source)
random.Shuffle(len(randomList), func(i, j int) { randomList[i], randomList[j] = randomList[j], randomList[i] })
```

## Others
### Variable Length Integer Encoding
Variable Length Integer (varint) is a way of encoding integers using a variable number of bytes to save space. There are two common ways: length-prefixed and continuation-bit.
