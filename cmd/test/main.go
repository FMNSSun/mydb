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

	put([]byte("hello"), []byte("world"), "localhost:10001")
	get([]byte("hello"), "localhost:10002")
}

func put(key, value []byte, raddr string) {
	mconn, err := DialMessageConn(raddr)

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	err = mconn.SendMessage(&Put{
		Key: key,
		Value: value,
	})

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	msg, err := mconn.ReadMessage()

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	log.Printf("Msg: %s", msg)
}

func get(key []byte, raddr string) {
	mconn, err := DialMessageConn(raddr)

	err = mconn.SendMessage(&Get{
		Key: key,
	})

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	msg, err := mconn.ReadMessage()

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	log.Printf("Msg: %s", msg)
}
