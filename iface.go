package mydb

import (
	"crypto/sha1"
	"fmt"
)

type MessageConn interface {
	SendMessage(msg Message) error
	ReadMessage() (Message, error)
	Begin()
	End()
}

type KeyHash [20]byte

func Hash(key []byte) KeyHash {
	return sha1.Sum(key)
}

type Engine interface {
	Serve(laddr string) error
	AddReplica(raddr string) error
}

type Storage interface {
	Put(key []byte, value []byte) StorageError
	Get(key []byte) ([]byte, StorageError)
}

type EngineError interface {
	error
	ErrCode() uint8
	Msg() string
	Cause() error
}

// Too much stuff going on. 
const ERR_TOOBUSY = uint8(0x01)

// Generic internal error.
const ERR_INTERNAL = uint8(0x02)

// Generic error on replicate.
const ERR_REPLICATE = uint8(0x03)

// Generic storage error
const ERR_STORAGE = uint8(0x04)

type engineError struct {
	errCode uint8
	msg string
	cause error
}

func (ee *engineError) ErrCode() uint8 {
	return ee.errCode
}

func (ee *engineError) Msg() string {
	return ee.msg
}

func (ee *engineError) Cause() error {
	return ee.cause
}

func (ee *engineError) Error() string {
	if ee.cause != nil {
		return fmt.Sprintf("ERR(%d): %s (Cause: %s)", ee.errCode, ee.msg, ee.cause.Error())
	} else {
		return fmt.Sprintf("ERR(%d): %s", ee.errCode, ee.msg)
	}
}

func EngineErrorf2(errCode uint8, cause error, msg string, args... interface{}) EngineError {
	return &engineError {
		errCode: errCode,
		msg: fmt.Sprintf(msg, args...),
		cause: cause,
	}
}

func EngineErrorf(errCode uint8, msg string, args... interface{}) EngineError {
	return &engineError {
		errCode: errCode,
		msg: fmt.Sprintf(msg, args...),
	}
}

type StorageError interface {
	error
	ErrCode() uint8
	Msg() string
	Cause() error
}

// Entry does not exist.
const ERR_NOTEXISTS = uint8(0xC0)


type storageError struct {
	errCode uint8
	msg string
	cause error
}

func (se *storageError) ErrCode() uint8 {
	return se.errCode
}

func (se *storageError) Msg() string {
	return se.msg
}

func (se *storageError) Cause() error {
	return se.cause
}

func (se *storageError) Error() string {
	if se.cause != nil {
		return fmt.Sprintf("ERR(%d): %s (Cause: %s)", se.errCode, se.msg, se.cause.Error())
	} else {
		return fmt.Sprintf("ERR(%d): %s", se.errCode, se.msg)
	}
}

func StorageErrorf(errCode uint8, msg string, args... interface{}) StorageError {
	return &storageError {
		errCode: errCode,
		msg: fmt.Sprintf(msg, args...),
	}
}
