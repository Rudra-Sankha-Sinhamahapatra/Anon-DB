package main

import "C"
import "github.com/Rudra-Sankha-Sinhamahapatra/Anon-DB/store"

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

func main() {}
