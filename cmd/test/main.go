package main

import (
	. "github.com/FMNSSun/mydb"
	. "github.com/FMNSSun/mydb/storage"
	"log"
	"time"
)


func main() {
	engine1 := NewEngine(NewMemoryStorage())

	go func() {
		err := engine1.Serve(":10001")
		log.Fatal(err.Error())
	}()

	engine2 := NewEngine(NewMemoryStorage())

	go func() {
		err := engine2.Serve(":10002")
		log.Fatal(err.Error())
	}()

	time.Sleep(2 * time.Second)

	err := engine1.AddReplica("localhost:10002")

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

	client, err = NewClient("localhost:10002")

	if err != nil {
		log.Fatal(err.Error())
	}

	val, err := client.Get([]byte("hello"))

	if err != nil {
		log.Fatal(err.Error())
	}

	log.Print(val)
}
