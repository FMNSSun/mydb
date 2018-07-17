package main

import (
	. "github.com/FMNSSun/mydb"
	. "github.com/FMNSSun/mydb/storage"
	"log"
	"time"
	"os"
)


func main() {
	logger1 := NewLogger(os.Stderr, "E1 > ")

	engine1 := NewEngine(NewMemoryStorage(logger1), logger1)

	go func() {
		err := engine1.Serve(":10001")
		logger1.Fatal(err.Error())
	}()

	logger2 := NewLogger(os.Stderr, "E2 > ")

	engine2 := NewEngine(NewMemoryStorage(logger2), logger2)

	go func() {
		err := engine2.Serve(":10002")
		logger2.Fatal(err.Error())
	}()

	logger3 := NewLogger(os.Stderr, "E3 > ")

	engine3 := NewEngine(NewMemoryStorage(logger3), logger3)

	go func() {
		err := engine3.Serve(":10003")
		logger3.Fatal(err.Error())
	}()

	time.Sleep(2 * time.Second)

	err := engine1.AddReplica("localhost:10002")

	if err != nil {
		log.Fatal(err.Error())
	}

	err = engine3.AddLookup("localhost:10002")

	if err != nil {
		log.Fatal(err.Error())
	}

	client, err := NewClient("localhost:10001")

	if err != nil {
		log.Fatal(err.Error())
	}

	err = client.Put([]byte("hello"), []byte("world"))

	if err != nil {
		log.Fatal(err.Error())
	}

	client, err = NewClient("localhost:10003")

	if err != nil {
		log.Fatal(err.Error())
	}

	val, err := client.Get([]byte("hello"))

	if err != nil {
		log.Fatal(err.Error())
	}

	log.Print(val)
}
