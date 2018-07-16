package mydb

type QMessage struct {
	Msg Message
	QResultChan chan *QResult
}

type QResult struct {
	Err error
	Msg Message
}
