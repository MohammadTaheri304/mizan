package main

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
)

type DataEntry struct {
	Key        string  `json:"key"`
	Value      string  `json:"value"`
	State      string  `json:"state"`
	Confidence float32 `json:"confidence"`
}

func handleClusterSet(nc *nats.Conn, jibmem *MDB) (*nats.Subscription, error) {
	return nc.Subscribe(CLUSTER_SET_SUBJECT, func(m *nats.Msg) {
		//log.Printf("Received a set message: %s\n", string(m.Data))

		go func() {
			var s DataEntry
			err := json.Unmarshal(m.Data, &s)
			checkError(err, "Unmarshal cluster set request")
			res, err := jibmem.set(s.Key, s.Value)
			if err != nil {
				return
			}
			output, err := json.Marshal(DataEntry{Key: res.key, Value: res.value, State: res.state})
			checkError(err, "Marshal cluster set response")
			nc.Publish(m.Reply, output)
		}()
	})
}

func handleClusterGet(nc *nats.Conn, jibmem *MDB) (*nats.Subscription, error) {
	return nc.Subscribe(CLUSTER_GET_SUBJECT, func(m *nats.Msg) {
		//log.Printf("Received a get message: %s\n", string(m.Data))

		go func() {
			var s DataEntry
			err := json.Unmarshal(m.Data, &s)
			checkError(err, "Unmarshal cluster get request")
			res, _ := jibmem.get(s.Key)
			output, err := json.Marshal(DataEntry{Key: res.key, Value: res.value, State: res.state})
			checkError(err, "Marshal cluster get response")
			nc.Publish(m.Reply, output)
		}()
	})
}
