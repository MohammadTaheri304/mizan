package main

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
	"log"
)

type DataEntry struct {
	OpCode     string  `json:"opcode"`
	Key        string  `json:"key"`
	Value      string  `json:"value"`
	State      string  `json:"state"`
	Confidence float32 `json:"confidence"`
}

func handleClusterRequest(nc *nats.Conn, jibmem *MDB) (*nats.Subscription, error) {
	return nc.Subscribe(CLUSTER_REQUEST_SUBJECT, func(m *nats.Msg) {
		//log.Printf("Received a set message: %s\n", string(m.Data))
		go func() {
			var s DataEntry
			err := json.Unmarshal(m.Data, &s)
			checkError(err, "Unmarshal cluster set request")

			switch s.OpCode {
			case "SET":
				handleClusterSet(s, m.Reply, jibmem, nc)
			case "GET":
				handleClusterGetRequest(s, m.Reply, jibmem, nc)
			default:
				log.Print("Request opcode not matched with supported ones!!!")
			}
		}()
	})
}

func handleClusterSet(request DataEntry, replyTo string, jibmem *MDB, nc *nats.Conn) {
	res, err := jibmem.set(request.Key, request.Value)
	if err != nil {
		return
	}
	output, err := json.Marshal(DataEntry{Key: res.key, Value: res.value, State: res.state})
	checkError(err, "Marshal cluster set response")
	nc.Publish(replyTo, output)
}

func handleClusterGetRequest(request DataEntry, replyTo string, jibmem *MDB, nc *nats.Conn) {
	res, _ := jibmem.get(request.Key)
	output, err := json.Marshal(DataEntry{Key: res.key, Value: res.value, State: res.state})
	checkError(err, "Marshal cluster get response")
	nc.Publish(replyTo, output)
}
