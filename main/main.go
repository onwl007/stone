package main

import (
	"fmt"
	"log"

	"github.com/onwl007/stone"
)

func main() {
	db, err := stone.Open("./stone")
	if err != nil {
		log.Fatal(err)
	}

	k := []byte("hello")
	v := []byte("world")
	err = db.Put(k, v)

	vs, err := db.Get(k)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("get value: ", string(vs))
}
