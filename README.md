# Anon-DB

Anon-DB is a simple in-memory key-value store written in Go. It provides basic operations such as setting, getting, and deleting key-value pairs. It also supports concurrent access.

## Installation

To install Anon-DB, use `go get`:

```sh
go get github.com/Rudra-Sankha-Sinhamahapatra/Anon-DB
```

## Usage

Here is an example of how to use Anon-DB:

```go
package main

import (
	"fmt"
	"github.com/Rudra-Sankha-Sinhamahapatra/Anon-DB/store"
)

func main() {
	store := store.NewInMemoryStore()

	// Set a key-value pair
	store.Set("foo", []byte("bar"))

	// Get the value for a key
	value, err := store.Get("foo")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Value:", string(value))
	}

	// Delete a key
	store.Delete("foo")
}
```

## Features

- **Set**: Add or update a key-value pair.
- **Get**: Retrieve the value associated with a key.
- **Delete**: Remove a key-value pair.
- **Concurrent Access**: Supports concurrent read and write operations.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.