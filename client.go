package mydb

import (
	"log"
)

type Client struct {
	mconn MessageConn
}

func NewClient(raddr string, logger Logger) (*Client, error) {
	mconn, err := DialMessageConn(raddr, logger)

	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		return nil, err
	}

	return &Client{mconn: mconn}, nil
}

func (c *Client) Get(key []byte) ([]byte, ClientError) {
	c.mconn.Begin()
	defer c.mconn.End()

	getMsg := &Get{
		Key: key,
	}

	err := c.mconn.SendMessage(getMsg)

	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		return nil, ClientErrorf2(err, "Network error.")
	}

	msg, err := c.mconn.ReadMessage()

	if err != nil {
		log.Printf("ERROR: %s", err.Error())
		return nil, ClientErrorf2(err, "Network error.")
	}

	switch msg.(type) {
	case *Result:
		resultMsg := msg.(*Result)

		if resultMsg.MId != getMsg.MId {
			return nil, ClientErrorf("Server responded with wrong message id.")
		}

		return resultMsg.Data, nil
	case *Status:
		statusMsg := msg.(*Status)

		if statusMsg.MId != getMsg.MId {
			return nil, ClientErrorf("Server responded with wrong message id.")
		}

		if statusMsg.StatusCode != 0 {
			return nil, ClientErrorf("Server responded with error %d", statusMsg.StatusCode)
		}

		return nil, ClientErrorf("Server responded with wrong message type.")
	default:
		return nil, ClientErrorf("Server responded with wrong message type.")
	}
}

func (c *Client) Put(key, value []byte) ClientError {
	c.mconn.Begin()
	defer c.mconn.End()

	putMsg := &Put{
		Key: key,
		Value: value,
	}

	err := c.mconn.SendMessage(putMsg)

	if err != nil {
		log.Printf("[CLIENT] ERROR: %s", err.Error())
		return ClientErrorf2(err, "Network error.")
	}

	msg, err := c.mconn.ReadMessage()

	if err != nil {
		log.Printf("[CLIENT] ERROR: %s", err.Error())
		return ClientErrorf2(err, "Network error.")
	}

	switch msg.(type) {
	case *Status:
		statusMsg := msg.(*Status)

		if statusMsg.MId != putMsg.MId {
			return ClientErrorf("Server responded with wrong message id.")
		}

		if statusMsg.StatusCode != 0 {
			return ClientErrorf("Server responded with error %d", statusMsg.StatusCode)
		}

		return nil
	default:
		return ClientErrorf("Server responded with wrong message type.")
	}
}
