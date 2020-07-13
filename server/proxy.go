package main

import (
	"encoding/json"
	"errors"
	"github.com/MohammadTaheri304/mizan/shared"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

func handleClientRequest(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.QueueSubscribe(shared.CLIENT_REQUEST_SUBJECT, "proxy", func(m *nats.Msg) {
		go handleClusterRequest_majority(m, nc)
	})
}

func handleClusterRequest_majority(m *nats.Msg, nc *nats.Conn) {
	respList := requestCluster(m, nc)
	res, err := findMajority(respList)
	if err != nil {
		log.Println("Error in find majority: ", err, ". request ", string(m.Data), "clusterResponses ", respList)
		return
	}
	output, err := json.Marshal(res)
	shared.CheckError(err, "Marshal cluster majority response")
	if res.Confidence < 1 {
		err = nc.Publish(shared.CLUSTER_SYNC_SUBJECT, output)
		shared.CheckError(err, "Publish cluster sync")
	}

	err = nc.Publish(m.Reply, output)
	shared.CheckError(err, "Publish cluster majority response")
}

func requestCluster(m *nats.Msg, nc *nats.Conn) []shared.DataEntry {
	resSubject := "mizan.cluster.req.res." + uuid.New().String()
	resChan := make(chan shared.DataEntry, 1000)
	subscription, err := nc.Subscribe(resSubject, func(msg *nats.Msg) {
		var s shared.DataEntry
		err := json.Unmarshal(msg.Data, &s)
		shared.CheckError(err, "Unmarshal cluster req.res response")
		resChan <- s
	})
	shared.CheckError(err, "Register subscriber for cluster req.res response")
	defer subscription.Unsubscribe()

	err = nc.PublishRequest(shared.CLUSTER_REQUEST_SUBJECT, resSubject, m.Data)
	shared.CheckError(err, "Publish cluster req.res request")

	respList := []shared.DataEntry{}
	tk := time.NewTicker(250 * time.Millisecond)
	wCounter := 8
ll:
	for {
		select {
		case cr := <-resChan:
			respList = append(respList, cr)
		case <-tk.C:
			if len(respList)%2 == 0 && wCounter > 0 {
				wCounter -= 1
			} else {
				break ll
			}
		}
	}
	return respList
}

func findMajority(respList []shared.DataEntry) (shared.DataEntry, error) {
	votMap := make(map[string]int)
	max := 0
	var maxItem shared.DataEntry
	for _, entry := range respList {
		votMap[entry.State] += 1
		if max < votMap[entry.State] {
			max = votMap[entry.State]
			maxItem = entry
		}
	}

	if max <= len(respList)/2 {
		return shared.DataEntry{}, errors.New("no.majority")
	}
	maxItem.Confidence = float32(max) / float32(len(respList))
	return maxItem, nil
}
