package main

import (
	. "github.com/FMNSSun/mydb"
	. "github.com/FMNSSun/mydb/storage"
	"log"
	"time"
	"fmt"
	"os"
	"io"
	"flag"
	"sync"
)


func main() {

	var lout io.Writer = nil

	doLog := flag.Bool("log", false, "Log?")
	num := flag.Int("num", 1024*128, "Iterations?")

	flag.Parse()

	if *doLog {
		lout = os.Stderr
	}

	N := *num

	logger1 := NewLogger(lout, "E1 > ")

	storage1, err := NewSqliteStorage("./sqlite/db1.db", logger1)

	if err != nil {
		logger1.Fatal(err.Error())
	}

	engine1 := NewEngine(storage1, logger1)

	go func() {
		err := engine1.Serve(":10001")
		logger1.Fatal(err.Error())
	}()

	logger2 := NewLogger(lout, "E2 > ")

	storage2, err := NewSqliteStorage("./sqlite/db2.db", logger2)

	if err != nil {
		logger2.Fatal(err.Error())
	}

	engine2 := NewEngine(storage2, logger2)

	go func() {
		err := engine2.Serve(":10002")
		logger2.Fatal(err.Error())
	}()

	time.Sleep(2 * time.Second)

	err = engine1.AddReplica("localhost:10002")

	if err != nil {
		log.Fatal(err.Error())
	}

	client1, err := NewClient("localhost:10001", NewLogger(lout, "C >"))

	if err != nil {
		log.Fatal(err.Error())
	}

	client2, err := NewClient("localhost:10001", NewLogger(lout, "C >"))

	if err != nil {
		log.Fatal(err.Error())
	}

	now := time.Now().UnixNano()

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		key := []byte{0x00, 0}
		data := []byte("hello world this is a somewhat decently long sentence with some letters in it.")

		for i := 0; i < N; i++ {

			putErr := client1.Put(key, data)

			if putErr != nil {
				log.Fatal(err.Error())
			}

			key[1]++
		}

		wg.Done()
	}()

	go func() {
		key := []byte{0xFF, 0}
		data := []byte("hello world this is a somewhat decently long sentence with some letters in it.")

		for i := 0; i < N; i++ {

			putErr := client2.Put(key, data)

			if putErr != nil {
				log.Fatal(err.Error())
			}

			key[1]++
		}

		wg.Done()
	}()

	wg.Wait()

	diff := time.Now().UnixNano() - now
	diffs := float64(diff) / float64(1000000000)
	opsps := float64(N) / float64(diffs)

	fmt.Printf("PUT: %f ops/s (%f seconds)\n", opsps, diffs)

	now = time.Now().UnixNano()

	wg.Add(2)

	go func() {
		key := []byte{0x00, 0}

		for i := 0; i < N; i++ {

			_, getErr := client1.Get(key)

			if getErr != nil {
				log.Fatal(err.Error())
			}

			key[1]++
		}

		wg.Done()
	}()

	go func() {
		key := []byte{0xFF, 0}

		for i := 0; i < N; i++ {

			_, getErr := client2.Get(key)

			if getErr != nil {
				log.Fatal(err.Error())
			}

			key[1]++
		}

		wg.Done()
	}()

	wg.Wait()

	diff = time.Now().UnixNano() - now
	diffs = float64(diff) / float64(1000000000)
	opsps = float64(N) / float64(diffs)

	fmt.Printf("GET: %f ops/s (%f seconds)\n", opsps, diffs)

}
