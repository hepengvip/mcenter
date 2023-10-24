package mcenterproto

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	SUCCESS     = 0
	USER_EXIST  = 100001
	BAD_REQUEST = 999998
	BAD_REPLY   = 999999
)

func Parse(data []byte) (*Message, error) {

	// remove white space
	line := string(data)
	cmdStr := strings.Trim(line, " \t\r\n")

	// split
	parts := strings.SplitN(cmdStr, " ", 4)
	cmd := parts[0]

	// parse publish request
	if cmd == MSG_PUBLISH {
		return ParsePublish(cmdStr, parts)
	}

	// parse message
	if cmd == MSG_MESSAGE {
		return ParseMessage(cmdStr, parts)
	}

	// parse reply message
	if cmd == MSG_REPLY {
		return ParseReply(cmdStr, parts)
	}

	// parse other request
	var reqId, payLoad string
	switch len(parts) {
	case 2:
		reqId, payLoad = "", parts[1]
	case 3:
		reqId, payLoad = parts[1], parts[2]
	default:
		return nil, fmt.Errorf("%d:%s", BAD_REQUEST, cmdStr)
	}

	switch cmd {
	case MSG_SET_USER:
		return &Message{
			ReqType: MSG_SET_USER,
			ReqId:   reqId,
			UserId:  payLoad,
		}, nil
	case MSG_NEW_CHANNEL:
		return &Message{
			ReqType: MSG_NEW_CHANNEL,
			ReqId:   reqId,
			Channel: payLoad,
		}, nil
	case MSG_SUBSCRIBE:
		return &Message{
			ReqType: MSG_SUBSCRIBE,
			ReqId:   reqId,
			Channel: payLoad,
		}, nil
	case MSG_UNSUBSCRIBE:
		return &Message{
			ReqType: MSG_UNSUBSCRIBE,
			ReqId:   reqId,
			Channel: payLoad,
		}, nil
	default:
		return nil, fmt.Errorf("%d:%s", BAD_REQUEST, cmdStr)
	}
}

func ParseMessage(cmdStr string, parts []string) (*Message, error) {
	if len(parts) != 4 {
		return nil, fmt.Errorf("%d:%s", BAD_REQUEST, cmdStr)
	}

	userId, channel, msgSizeStr := parts[1], parts[2], parts[3]

	msgSize, err := strconv.Atoi(msgSizeStr)
	if err != nil {
		return nil, fmt.Errorf("%d:message size field parse error - %s", BAD_REQUEST, cmdStr)
	}

	payload := make([]byte, msgSize)

	return &Message{
		ReqType:     MSG_MESSAGE,
		UserId:      userId,
		Channel:     channel,
		PayloadSize: msgSize,
		Payload:     &payload,
	}, nil
}

func ParsePublish(cmdStr string, parts []string) (*Message, error) {
	var reqId, channel, msgSizeStr string
	switch len(parts) {
	case 3:
		reqId, channel, msgSizeStr = "", parts[1], parts[2]
	case 4:
		reqId, channel, msgSizeStr = parts[1], parts[2], parts[3]
	default:
		return nil, fmt.Errorf("%d:%s", BAD_REQUEST, cmdStr)
	}

	msgSize, err := strconv.Atoi(msgSizeStr)
	if err != nil {
		return nil, fmt.Errorf("%d:message size field parse error - %s", BAD_REQUEST, cmdStr)
	}

	payload := make([]byte, msgSize)

	return &Message{
		ReqType:     MSG_PUBLISH,
		ReqId:       reqId,
		Channel:     channel,
		PayloadSize: msgSize,
		Payload:     &payload,
	}, nil
}

func ParseReply(cmdStr string, parts []string) (*Message, error) {

	var reqId, payload string
	if len(parts) == 2 {
		reqId, payload = "", parts[1]
	} else if len(parts) == 3 {
		reqId, payload = parts[1], parts[2]
	} else if len(parts) == 4 {
		reqId, payload = parts[1], fmt.Sprintf("%s %s", parts[2], parts[3])
	} else {
		return nil, fmt.Errorf("%d:%s", BAD_REPLY, cmdStr)
	}

	payloadParts := strings.SplitN(payload, ":", 2)
	if len(payloadParts) != 2 {
		return nil, fmt.Errorf("%d:%s", BAD_REPLY, cmdStr)
	}

	code, err := strconv.Atoi(payloadParts[0])
	if err != nil {
		return nil, fmt.Errorf("reply code parse failed - %s", cmdStr)
	}

	payloadStr := []byte(payloadParts[1])

	return &Message{
		ReqType:     MSG_REPLY,
		ReqId:       reqId,
		PayloadSize: code,
		Payload:     &payloadStr,
	}, nil
}
