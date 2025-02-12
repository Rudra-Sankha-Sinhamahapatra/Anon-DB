package main

import (
	"fmt"

	"github.com/Rudra-Sankha-Sinhamahapatra/Anon-DB/store"
)

func main() {
	store := store.NewInMemoryStore()

	store.Set("foo", []byte("hii"))
	store.Set("io", []byte("hello"))

	value, err := store.Get("foo")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Value:", string(value))
	}

	val, err := store.Get("io")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Value 2:", string(val))
	}

	store.Delete("foo")
}
