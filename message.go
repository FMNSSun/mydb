package mydb

import (
	"fmt"
	"encoding/binary"
	"io"
	"bytes"
)

type Message interface {
	Id() uint32
	Type() uint8
	String() string
}

type Status struct {
	MId uint32
	StatusCode uint8
}

func (s *Status) String() string {
	return fmt.Sprintf("STATUS %d %d", s.MId, s.StatusCode)
}

func (s *Status) Id() uint32 {
	return s.MId
}

func (*Status) Type() uint8 {
	return MTYPE_STATUS
}

type Result struct {
	MId uint32
	Data []byte
}

func (r *Result) String() string {
	return fmt.Sprintf("RESULT %d %x", r.MId, r.Data)
}

func (r *Result) Id() uint32 {
	return r.MId
}

func (*Result) Type() uint8 {
	return MTYPE_RESULT
}

type Get struct {
	MId uint32
	Key []byte
}

func (g *Get) String() string {
	return fmt.Sprintf("GET %d %x", g.MId, g.Key)
}

func (g *Get) Id() uint32 {
	return g.MId
}

func (*Get) Type() uint8 {
	return MTYPE_GET
}

type Put struct {
	MId uint32
	Key []byte
	Value []byte
}

func (p *Put) String() string {
	return fmt.Sprintf("PUT %d %x %x", p.MId, p.Key, p.Value)
}

func (p *Put) Id() uint32 {
	return p.MId
}

func (*Put) Type() uint8 {
	return MTYPE_PUT
}

const MTYPE_PUT = uint8(0x01)
const MTYPE_GET = uint8(0x02)
const MTYPE_STATUS = uint8(0x03)
const MTYPE_RESULT = uint8(0x04)

func WriteMessage(w io.Writer, msg Message) error {
	switch msg.(type) {
	case *Put:
		putMsg := msg.(*Put)
		return writePutMessage(w, putMsg)
	case *Get:
		getMsg := msg.(*Get)
		return writeGetMessage(w, getMsg)
	case *Status:
		statusMsg := msg.(*Status)
		return writeStatusMessage(w, statusMsg)
	case *Result:
		resultMsg := msg.(*Result)
		return writeResultMessage(w, resultMsg)
	}

	return fmt.Errorf("Unknown message type (w).")
}

func writePutMessage(w io.Writer, putMsg *Put) error {
	buf := new(bytes.Buffer)
	payloadLength := len(putMsg.Key) + 2 + len(putMsg.Value) + 4
	binary.Write(buf, binary.LittleEndian, putMsg.MId)
	binary.Write(buf, binary.LittleEndian, MTYPE_PUT)
	binary.Write(buf, binary.LittleEndian, uint32(payloadLength))
	binary.Write(buf, binary.LittleEndian, uint16(len(putMsg.Key)))
	buf.Write(putMsg.Key)
	binary.Write(buf, binary.LittleEndian, uint32(len(putMsg.Value)))
	buf.Write(putMsg.Value)
	data := buf.Bytes()

	if len(data) != (payloadLength + 9) {
		panic("BUG: writePutMessage... length is wrong!")
	}

	_, err := w.Write(data)

	return err
}

func writeGetMessage(w io.Writer, getMsg *Get) error {
	buf := new(bytes.Buffer)
	payloadLength := len(getMsg.Key) + 2
	binary.Write(buf, binary.LittleEndian, getMsg.MId)
	binary.Write(buf, binary.LittleEndian, MTYPE_GET)
	binary.Write(buf, binary.LittleEndian, uint32(payloadLength))
	binary.Write(buf, binary.LittleEndian, uint16(len(getMsg.Key)))
	buf.Write(getMsg.Key)
	data := buf.Bytes()

	if len(data) != (payloadLength + 9) {
		panic("BUG: writeGetMessage... length is wrong!")
	}

	_, err := w.Write(data)

	return err
}

func writeStatusMessage(w io.Writer, statusMsg *Status) error {
	buf := new(bytes.Buffer)
	payloadLength := 1
	binary.Write(buf, binary.LittleEndian, statusMsg.MId)
	binary.Write(buf, binary.LittleEndian, MTYPE_STATUS)
	binary.Write(buf, binary.LittleEndian, uint32(payloadLength))
	buf.WriteByte(statusMsg.StatusCode)
	data := buf.Bytes()

	if len(data) != (payloadLength + 9) {
		panic("BUG: writeOkMessage... length is wrong!")
	}

	_, err := w.Write(data)

	return err
}

func writeResultMessage(w io.Writer, resultMsg *Result) error {
	buf := new(bytes.Buffer)
	payloadLength := len(resultMsg.Data) + 4
	binary.Write(buf, binary.LittleEndian, resultMsg.MId)
	binary.Write(buf, binary.LittleEndian, MTYPE_RESULT)
	binary.Write(buf, binary.LittleEndian, uint32(payloadLength))
	binary.Write(buf, binary.LittleEndian, uint32(len(resultMsg.Data)))
	buf.Write(resultMsg.Data)
	data := buf.Bytes()

	if len(data) != (payloadLength + 9) {
		panic("BUG: writeResultMessage... length is wrong!")
	}

	_, err := w.Write(data)

	return err
}

func CreateMessage(mid uint32, mtype uint8, payload []byte) (Message, error) {
	switch mtype {
	case MTYPE_PUT:
		return putMessage(mid, payload)
	case MTYPE_GET:
		return getMessage(mid, payload)
	case MTYPE_STATUS:
		return statusMessage(mid, payload)
	case MTYPE_RESULT:
		return resultMessage(mid, payload)
	}

	return nil, fmt.Errorf("Unknown message type (r).")
}

func resultMessage(mid uint32, payload []byte) (Message, error) {
	if ulen(payload) < 4 {
		return nil, fmt.Errorf("Payload too small for Result message. Missing data length.")
	}

	dataLen := binary.LittleEndian.Uint32(payload[0:])

	payload = payload[4:]

	if ulen(payload) != dataLen {
		return nil, fmt.Errorf("Payload of wrong size for Result message. Wrong data length.")
	}

	return &Result{MId: mid, Data: payload}, nil
}

func statusMessage(mid uint32, payload []byte) (Message, error) {
	if ulen(payload) < 1 {
		return nil, fmt.Errorf("Payload too small for Status message.")
	}

	statusCode := payload[0]

	if len(payload) > 1 {
		return nil, fmt.Errorf("Payload too big for Status message. Trailing bytes detected.")
	}

	return &Status{MId: mid, StatusCode: statusCode}, nil
}

func getMessage(mid uint32, payload []byte) (Message, error) {
	if ulen(payload) < 2 {
		return nil, fmt.Errorf("Payload too small for Get message. Missing key length.")
	}

	keyLen := uint32(binary.LittleEndian.Uint16(payload[0:]))

	payload = payload[2:]

	if ulen(payload) < keyLen {
		return nil, fmt.Errorf("Payload too small for Get message. Missing key bytes.")
	}

	keyBytes := payload[:keyLen]

	payload = payload[keyLen:]

	if len(payload) != 0 {
		return nil, fmt.Errorf("Payload too big for Get message. Trailing bytes detected.")
	}

	return &Get{MId: mid, Key: keyBytes}, nil
}

func putMessage(mid uint32, payload []byte) (Message, error) {
	if ulen(payload) < 2 {
		return nil, fmt.Errorf("Payload too small for Put message. Missing key length.")
	}

	keyLen := uint32(binary.LittleEndian.Uint16(payload[0:]))

	payload = payload[2:]

	if ulen(payload) < keyLen {
		return nil, fmt.Errorf("Payload too small for Put message. Missing key bytes.")
	}

	keyBytes := payload[:keyLen]

	payload = payload[keyLen:]

	if ulen(payload) < 4 {
		return nil, fmt.Errorf("Payload too small for Put message. Missing value length.")
	}

	valueLen := binary.LittleEndian.Uint32(payload[0:])

	payload = payload[4:]

	if ulen(payload) < valueLen {
		return nil, fmt.Errorf("Payload too small for Put message. Missing value bytes.")
	}

	valueBytes := payload[:valueLen]

	payload = payload[valueLen:]

	if len(payload) != 0 {
		return nil, fmt.Errorf("Payload too big for Put message. Trailing bytes detected.")
	}

	return &Put{MId: mid, Key : keyBytes, Value : valueBytes}, nil
}
