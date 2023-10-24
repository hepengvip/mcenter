package mcenterproto

import "fmt"

const REP_PREFIX = "REP"

type Response struct {
	ReqId   string
	ReqCode int
	ReqMsg  string
}

func (r *Response) ToBytes() []byte {
	prefix := fmt.Sprintf("%s", REP_PREFIX)
	if r.ReqId != "" {
		prefix = fmt.Sprintf("%s %s", REP_PREFIX, r.ReqId)
	}

	return []byte(fmt.Sprintf("%s %d:%s\n", prefix, r.ReqCode, r.ReqMsg))
}
