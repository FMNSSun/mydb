package mydb

import (
	"net"
	"sync"
	"math/rand"
)

type DefaultEngine struct {
	replicas []MessageConn
	lookups []MessageConn
	storage Storage
	mutex *sync.RWMutex
	logger Logger
}

func NewEngine(s Storage, logger Logger) Engine {
	return &DefaultEngine {
		storage: s,
		mutex: &sync.RWMutex{},
		logger: logger,
	}
}

func (de *DefaultEngine) AddReplica(raddr string) error {

	mconn, err := DialMessageConn(raddr)

	if err != nil {
		de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
		return err
	}

	de.mutex.Lock()
	defer de.mutex.Unlock()

	de.replicas = append(de.replicas, mconn)

	return nil
}

func (de *DefaultEngine) AddLookup(raddr string) error {

	mconn, err := DialMessageConn(raddr)

	if err != nil {
		de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
	}

	de.mutex.Lock()
	defer de.mutex.Unlock()

	de.lookups = append(de.lookups, mconn)

	return nil
}

func (de *DefaultEngine) Serve(laddr string) error {
	return de.acceptLoop(laddr)
}

func (de *DefaultEngine) acceptLoop(laddr string) error {
	de.logger.Out(LOGLVL_VERBOSE, "[ENGINE] begin acceptLoop")

	sck, err := net.Listen("tcp", laddr)

	if err != nil {
		de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
		return err
	}

	for {
		conn, err := sck.Accept()

		if err != nil {
			de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
			break
		}

		go de.connLoop(NewMessageConn(conn))
	}

	de.logger.Out(LOGLVL_VERBOSE, "[ENGINE] exit acceptLoop")
	return nil
}

func (de *DefaultEngine) connLoop(conn MessageConn) error {
	de.logger.Out(LOGLVL_VERBOSE, "[ENGINE] begin connLoop")

	for {
		msg, err := conn.ReadMessage()

		if err != nil {
			de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
			break
		}

		de.logger.Outf(LOGLVL_INFO, "[ENGINE] Message received: %s", msg)

		retMsg, perr := de.ProcessMessage(msg)

		if perr != nil {
			de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", perr.Error())
			conn.SendMessage(&Status{MId: msg.Id(), StatusCode: perr.ErrCode()})
			break
		}

		err = conn.SendMessage(retMsg)

		if err != nil {
			de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
			break
		}
	}

	de.logger.Out(LOGLVL_VERBOSE, "[ENGINE] exit connLoop")
	return nil
}

func (de *DefaultEngine) Lookup(getMsg *Get) ([]byte, EngineError) {
	de.mutex.RLock()
	lookups := de.lookups
	de.mutex.RUnlock()

	if len(lookups) == 0 {
		return nil, nil
	}

	var err error = nil

	for i := 0; i < len(lookups); i++ {
		j := rand.Intn(len(lookups))

		lookup := lookups[j]

		lookup.Begin()

		lookup.SendMessage(getMsg)

		retMsg, errNew := lookup.ReadMessage()

		lookup.End()

		if errNew != nil {
			de.logger.Outf(LOGLVL_ERROR, "[ENGINE] ERROR: %s", err.Error())
			err = errNew
		} else {
			switch retMsg.(type) {
			case *Status:
				err = EngineErrorf(ERR_LOOKUP, "Error on lookup: Status code received was %d.", retMsg.(*Status).StatusCode)
			case *Result:
				return retMsg.(*Result).Data, nil
			default:
				err = EngineErrorf(ERR_LOOKUP, "Server responded with wrong message type.")
			}
		}
	}

	return nil, EngineErrorf2(ERR_LOOKUP, err, "Lookup error.")
}

func (de *DefaultEngine) Replicate(putMsg *Put) EngineError {
	de.mutex.RLock()
	replicas := de.replicas
	de.mutex.RUnlock()

	for _, replica := range replicas {
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

		dataLocal, serr := de.storage.Get(key)

		if serr != nil {
			return nil, EngineErrorf2(ERR_STORAGE, serr, "Storage error.")
		}

		if dataLocal == nil {
			dataRemote, err := de.Lookup(getMsg)

			if err != nil {
				return nil, err
			}

			if dataRemote == nil {
				return &Status{MId: getMsg.MId, StatusCode: ERR_NOTEXISTS}, nil
			} else {
				return &Result{MId: getMsg.MId, Data: dataRemote}, nil
			}
		} else {
			return &Result{MId: getMsg.MId, Data: dataLocal}, nil
		}
	}

	return nil, EngineErrorf(ERR_INTERNAL, "Unknown message type.")
}
