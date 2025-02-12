package main

import "C"
import (
	"fmt"

	"github.com/Rudra-Sankha-Sinhamahapatra/Anon-DB/store"
)

var db = store.NewInMemoryStore()

func Set(key *C.char, value *C.char) {
	k := C.GoString(key)
	v := C.GoString(value)
	db.Set(k, []byte(v))
}

func Get(key *C.char) *C.char {
	k := C.GoString(key)
	value, err := db.Get(k)
	if err != nil {
		return C.CString("")
	}
	return C.CString(string(value))
}

func Delete(key *C.char) {
	k := C.GoString(key)
	db.Delete(k)
}

func main() {
	db := store.NewInMemoryStore()
	db.Set("name", []byte("Rudra"))

	val, err := db.Get("name")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Value:", string(val))
	}

	db.Delete("name")
}
