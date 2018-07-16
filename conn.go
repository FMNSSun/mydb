package mydb

import (
	"io"
	"net"
	"encoding/binary"
	"log"
	"sync"
)

type MyConn struct {
	conn net.Conn
	mutex *sync.Mutex
}


func NewMessageConn(conn net.Conn) MessageConn {
	return &MyConn {
			conn: conn,
			mutex: &sync.Mutex{},
	}
}

func DialMessageConn(raddr string) (MessageConn, error) {
	addr, err := net.ResolveTCPAddr("tcp", raddr)

	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return nil, err
	}

	return &MyConn {
		conn: conn,
		mutex: &sync.Mutex{},
	}, nil
}

func (mc *MyConn) Begin() {
	mc.mutex.Lock()
}

func (mc *MyConn) End() {
	mc.mutex.Unlock()
}

func (mc *MyConn) SendMessage(msg Message) error {
	return WriteMessage(mc.conn, msg)
}

func (mc *MyConn) ReadMessage() (Message, error) {
	header := make([]byte, 9)

	_, err := io.ReadFull(mc.conn, header)

	if err != nil {
		log.Printf("ERROR: header: %s", err.Error())
		return nil, err
	}

	mid := binary.LittleEndian.Uint32(header)
	mtype := header[4]
	lngth := binary.LittleEndian.Uint32(header[5:])

	payload := make([]byte, lngth)

	_, err = io.ReadFull(mc.conn, payload)

	if err != nil {
		log.Printf("ERROR: payload: %s", err.Error())
		return nil, err
	}

	return CreateMessage(mid, mtype, payload)
}
