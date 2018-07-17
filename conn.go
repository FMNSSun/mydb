package mydb

import (
	"io"
	"net"
	"encoding/binary"
	"sync"
)

type MyConn struct {
	conn net.Conn
	mutex *sync.Mutex
	logger Logger
}


func NewMessageConn(conn net.Conn, logger Logger) MessageConn {
	return &MyConn {
			conn: conn,
			mutex: &sync.Mutex{},
			logger: logger,
	}
}

func DialMessageConn(raddr string, logger Logger) (MessageConn, error) {
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
		logger: logger,
	}, nil
}

func (mc *MyConn) Begin() {
	mc.mutex.Lock()
}

func (mc *MyConn) End() {
	mc.mutex.Unlock()
}

func (mc *MyConn) Close() {
	mc.conn.Close()
}

func (mc *MyConn) CloseAndEnd() {
	mc.conn.Close()
	mc.mutex.Unlock()
}

func (mc *MyConn) SendMessage(msg Message) error {
	err := WriteMessage(mc.conn, msg)

	if err != nil {
		mc.logger.Outf(LOGLVL_ERROR, "[CONN] ERROR: send msg: %s", err.Error())
	}

	return err
}

func (mc *MyConn) ReadMessage() (Message, error) {
	header := make([]byte, 9)

	_, err := io.ReadFull(mc.conn, header)

	if err != nil {
		mc.logger.Outf(LOGLVL_ERROR, "[CONN] ERROR: recv header: %s", err.Error())
		return nil, err
	}

	mid := binary.LittleEndian.Uint32(header)
	mtype := header[4]
	lngth := binary.LittleEndian.Uint32(header[5:])

	payload := make([]byte, lngth)

	_, err = io.ReadFull(mc.conn, payload)

	if err != nil {
		mc.logger.Outf(LOGLVL_ERROR, "[CONN] ERROR: recv payload: %s", err.Error())
		return nil, err
	}

	return CreateMessage(mid, mtype, payload)
}
