package main

import (
	. "github.com/FMNSSun/mydb"
	"log"
)

func main() {
	raddr := "localhost:10001"

	mconn, err := DialMessageConn(raddr)

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	err = mconn.SendMessage(&Put{
		Key: []byte("hello"),
		Value: []byte("world"),
	})

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	msg, err := mconn.ReadMessage()

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	log.Printf("Msg: %s", msg)

	err = mconn.SendMessage(&Get{
		Key: []byte("hello"),
	})

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	msg, err = mconn.ReadMessage()

	if err != nil {
		log.Fatalf("ERROR: %s", err.Error())
	}

	log.Printf("Msg: %s", msg)
}
