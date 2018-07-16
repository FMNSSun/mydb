package mydb

import (
	"log"
	"net"
)

type DefaultEngine struct {
	replicas []MessageConn
	storage Storage
}

func NewEngine(s Storage) Engine {
	return &DefaultEngine {
		storage: s,
	}
}

func (de *DefaultEngine) AddReplica(raddr string) error {
	mconn, err := DialMessageConn(raddr)

	if err != nil {
		log.Printf("[ENGINE] ERROR: %s", err.Error())
		return err
	}

	de.replicas = append(de.replicas, mconn)

	return nil
}

func (de *DefaultEngine) Serve(laddr string) error {
	return de.acceptLoop(laddr)
}

func (de *DefaultEngine) acceptLoop(laddr string) error {
	log.Print("[ENGINE] begin acceptLoop")

	sck, err := net.Listen("tcp", laddr)

	if err != nil {
		log.Printf("[ENGINE] ERROR: %s", err.Error())
		return err
	}

	for {
		conn, err := sck.Accept()

		if err != nil {
			log.Printf("[ENGINE] ERROR: %s", err.Error())
			break
		}

		go de.connLoop(NewMessageConn(conn))
	}

	log.Print("[ENGINE] exit acceptLoop")
	return nil
}

func (de *DefaultEngine) connLoop(conn MessageConn) error {
	log.Print("[ENGINE] begin connLoop")

	for {
		msg, err := conn.ReadMessage()

		if err != nil {
			log.Printf("[ENGINE] ERROR: %s", err.Error())
			break
		}

		log.Printf("[ENGINE] Message received: %s", msg)

		retMsg, perr := de.ProcessMessage(msg)

		if perr != nil {
			log.Printf("[ENGINE] ERROR: %s", perr.Error())
			conn.SendMessage(&Status{MId: msg.Id(), StatusCode: perr.ErrCode()})
			break
		}

		err = conn.SendMessage(retMsg)

		if err != nil {
			log.Printf("[ENGINE] ERROR: %s", err.Error())
			break
		}
	}

	log.Print("[ENGINE] exit connLoop")
	return nil
}

func (de *DefaultEngine) Replicate(putMsg *Put) EngineError {
	for _, replica := range de.replicas {
		replica.Begin()

		replica.SendMessage(putMsg)
		retMsg, err := replica.ReadMessage()

		replica.End()

		if err != nil {
			return EngineErrorf(ERR_REPLICATE, "Error replicating to %s: %s", replica, err.Error())
		}

		if retMsg.Id() != putMsg.Id() {
			return EngineErrorf(ERR_REPLICATE, "Error replicating to %s: MessageIds don't match.", replica)
		}

		switch retMsg.(type) {
		case *Status:
			statusMsg := retMsg.(*Status)
			if statusMsg.StatusCode != 0 {
				return EngineErrorf(ERR_REPLICATE, "Error replicating to %s: Status code received was %d", replica, statusMsg.StatusCode)
			}
		default:
			return EngineErrorf(ERR_REPLICATE, "Wrong message received from %s.", replica)
		}
	}

	return nil
}

func (de *DefaultEngine) ProcessMessage(msg Message) (Message, EngineError) {
	switch msg.(type) {
	case *Put:
		putMsg := msg.(*Put)

		key := putMsg.Key
		value := putMsg.Value

		err := de.Replicate(putMsg)

		if err != nil {
			return nil, err
		}

		serr := de.storage.Put(key, value)

		if serr != nil {
			return nil, EngineErrorf2(ERR_STORAGE, serr, "Storage error.")
		}

		return &Status{MId: putMsg.MId, StatusCode: 0}, nil
	case *Get:
		getMsg := msg.(*Get)

		key := getMsg.Key

		data, serr := de.storage.Get(key)

		if serr != nil {
			if serr.ErrCode() == ERR_NOTEXISTS {
				return &Status{MId: getMsg.MId, StatusCode: serr.ErrCode()}, nil
			}
			return nil, EngineErrorf2(ERR_STORAGE, serr, "Storage error.")
		}

		return &Result{MId: getMsg.MId, Data: data}, nil
	}

	return nil, EngineErrorf(ERR_INTERNAL, "Unknown message type.")
}
