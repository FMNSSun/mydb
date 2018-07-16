package main

import (
	. "github.com/FMNSSun/mydb"
	. "github.com/FMNSSun/mydb/storage"
	"log"
)

func main() {
	e := NewEngine(NewMemoryStorage())
	err := e.Serve()
	log.Fatal(err.Error())
}
