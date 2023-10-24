package mcenterproto

import (
	"bufio"
	"fmt"
)

type Message struct {
	ReqType     string
	ReqId       string
	UserId      string
	Channel     string
	PayloadSize int
	Payload     *[]byte
}

func (m *Message) ToBytes() []byte {
	prefix := fmt.Sprintf("%s", m.ReqType)
	if m.ReqId != "" {
		prefix = fmt.Sprintf("%s %s", m.ReqType, m.ReqId)
	}

	switch m.ReqType {
	case MSG_SET_USER:
		return []byte(fmt.Sprintf("%s %s\n", prefix, m.UserId))
	case MSG_PUBLISH:
		header := []byte(fmt.Sprintf("%s %s %d\n", prefix, m.Channel, len(*m.Payload)))
		header = append(header, *m.Payload...)
		return header
	case MSG_MESSAGE:
		header := []byte(fmt.Sprintf("%s %s %s %d\n", m.ReqType, m.UserId, m.Channel, len(*m.Payload)))
		header = append(header, *m.Payload...)
		return header
	default:
		return []byte(fmt.Sprintf("%s %s\n", prefix, m.Channel))
	}
}

func (m *Message) ReadPayload(r *bufio.Reader) error {
	err := ReadFull(r, *m.Payload)
	if err != nil {
		return err
	}
	return nil
}

func (m *Message) ToReply() (*Response, error) {
	if m.ReqType != MSG_REPLY {
		return nil, fmt.Errorf("cannot convert %s message to reply", m.ReqType)
	}

	return &Response{
		ReqId:   m.ReqId,
		ReqCode: m.PayloadSize,
		ReqMsg:  string(*m.Payload),
	}, nil
}
