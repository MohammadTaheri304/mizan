package main

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

func handleClientRequest(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.QueueSubscribe(CLIENT_REQUEST_SUBJECT, "proxy", func(m *nats.Msg) {
		go handleClusterRequest_majority(m, nc)
	})
}

func handleClusterRequest_majority(m *nats.Msg, nc *nats.Conn) {
	respList := requestCluster(m, nc)
	res, err := findMajority(respList)
	if err != nil {
		log.Print("Error in find majority: %v. \n", err)
		return
	}
	output, err := json.Marshal(res)
	checkError(err, "Marshal cluster majority response")
	if res.Confidence < 1 {
		err = nc.Publish(CLUSTER_SYNC_SUBJECT, output)
		checkError(err, "Publish cluster sync")
	}

	err = nc.Publish(m.Reply, output)
	checkError(err, "Publish cluster majority response")
}

func requestCluster(m *nats.Msg, nc *nats.Conn) []DataEntry {
	resSubject := "mizan.cluster.req.res." + uuid.New().String()
	resChan := make(chan DataEntry, 1000)
	subscription, err := nc.Subscribe(resSubject, func(msg *nats.Msg) {
		var s DataEntry
		err := json.Unmarshal(msg.Data, &s)
		checkError(err, "Unmarshal cluster req.res response")
		resChan <- s
	})
	checkError(err, "Register subscriber for cluster req.res response")
	defer subscription.Unsubscribe()

	err = nc.PublishRequest(CLUSTER_REQUEST_SUBJECT, resSubject, m.Data)
	checkError(err, "Publish cluster req.res request")

	respList := []DataEntry{}
	tk := time.NewTicker(250 * time.Millisecond)
ll:
	for {
		select {
		case cr := <-resChan:
			respList = append(respList, cr)
		case <-tk.C:
			break ll
		}
	}
	return respList
}

func findMajority(respList []DataEntry) (DataEntry, error) {
	votMap := make(map[string]int)
	max := 0
	var maxItem DataEntry
	for _, entry := range respList {
		votMap[entry.State] += 1
		if max < votMap[entry.State] {
			max = votMap[entry.State]
			maxItem = entry
		}
	}

	if max <= len(respList)/2 {
		return DataEntry{}, errors.New("no.majority")
	}
	maxItem.Confidence = float32(max) / float32(len(respList))
	return maxItem, nil
}
