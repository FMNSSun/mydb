package main

import (
	. "github.com/FMNSSun/mydb"
	. "github.com/FMNSSun/mydb/storage"
	"log"
	"time"
	"fmt"
)


func main() {
	logger1 := NewLogger(nil, "E1 > ")

	engine1 := NewEngine(NewMemoryStorage(logger1), logger1)

	go func() {
		err := engine1.Serve(":10001")
		logger1.Fatal(err.Error())
	}()

	logger2 := NewLogger(nil, "E2 > ")

	engine2 := NewEngine(NewMemoryStorage(logger2), logger2)

	go func() {
		err := engine2.Serve(":10002")
		logger2.Fatal(err.Error())
	}()

	time.Sleep(2 * time.Second)

	err := engine1.AddReplica("localhost:10002")

	if err != nil {
		log.Fatal(err.Error())
	}

	client, err := NewClient("localhost:10002", NewLogger(nil, "C >"))

	if err != nil {
		log.Fatal(err.Error())
	}

	now := time.Now().UnixNano()
	N := 1024*256

	for i := 0; i < N; i++ {

		err = client.Put([]byte("hello"), []byte("world"))

		if err != nil {
			log.Fatal(err.Error())
		}

	}

	for i := 0; i < N; i++ {

		_, err = client.Get([]byte("hello"))

		if err != nil {
			log.Fatal(err.Error())
		}

	}

	diff := time.Now().UnixNano() - now
	diffs := float64(diff) / float64(1000000000)
	opsps := float64(N) / float64(diffs)

	fmt.Printf("%f ops/s (%f seconds)\n", opsps, diffs)
}
