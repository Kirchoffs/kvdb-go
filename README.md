# Notes

## Get Started
```
>> go mod init kvdb-go
>> go get github.com/google/btree
>> go get github.com/stretchr/testify
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
