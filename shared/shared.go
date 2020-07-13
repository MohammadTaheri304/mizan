package shared

import "log"

const CLIENT_REQUEST_SUBJECT = "mizan.client.request"

const CLUSTER_REQUEST_SUBJECT = "mizan.cluster.request"
const CLUSTER_SYNC_SUBJECT = "mizan.cluster.sync"
const CLUSTER_SYNC_REQUEST_SUBJECT = "mizan.cluster.sync.request"

type DataEntry struct {
	OpCode     string  `json:"opcode"`
	Key        string  `json:"key"`
	Value      string  `json:"value"`
	State      string  `json:"state"`
	Confidence float32 `json:"confidence"`
}

func CheckError(err error, desc string) {
	if err != nil {
		log.Fatalf("Error %v %v", desc, err)
	}
}

