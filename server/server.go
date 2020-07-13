package main

import (
	"encoding/json"
	"github.com/MohammadTaheri304/mizan/shared"
	"github.com/nats-io/nats.go"
	"log"
)

func handleClusterRequest(nc *nats.Conn, jibmem *MDB) (*nats.Subscription, error) {
	return nc.Subscribe(shared.CLUSTER_REQUEST_SUBJECT, func(m *nats.Msg) {
		//log.Printf("Received a set message: %s\n", string(m.Data))
		go func() {
			var s shared.DataEntry
			err := json.Unmarshal(m.Data, &s)
			shared.CheckError(err, "Unmarshal cluster set request")

			switch s.OpCode {
			case "SET":
				handleClusterSet(s, m.Reply, jibmem, nc)
			case "UNSET":
				handleClusterUnset(s, m.Reply, jibmem, nc)
			case "GET":
				handleClusterGetRequest(s, m.Reply, jibmem, nc)
			default:
				log.Print("Request opcode not matched with supported ones!!!")
			}
		}()
	})
}

func handleClusterSet(request shared.DataEntry, replyTo string, jibmem *MDB, nc *nats.Conn) {
	res, err := jibmem.set(request.Key, request.Value)
	if err != nil {
		return
	}
	output, err := json.Marshal(shared.DataEntry{Key: res.key, Value: res.value, State: res.state})
	shared.CheckError(err, "Marshal cluster set response")
	nc.Publish(replyTo, output)
}

func handleClusterUnset(request shared.DataEntry, replyTo string, jibmem *MDB, nc *nats.Conn) {
	res, err := jibmem.unset(request.Key)
	if err != nil {
		return
	}
	output, err := json.Marshal(shared.DataEntry{Key: res.key, Value: res.value, State: res.state})
	shared.CheckError(err, "Marshal cluster unset response")
	nc.Publish(replyTo, output)
}

func handleClusterGetRequest(request shared.DataEntry, replyTo string, jibmem *MDB, nc *nats.Conn) {
	res, _ := jibmem.get(request.Key)
	output, err := json.Marshal(shared.DataEntry{Key: res.key, Value: res.value, State: res.state})
	shared.CheckError(err, "Marshal cluster get response")
	nc.Publish(replyTo, output)
}
