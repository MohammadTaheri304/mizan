package main

import (
	"encoding/json"
	"github.com/MohammadTaheri304/mizan/shared"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

func handleClusterSync(nc *nats.Conn, jibmem *MDB) {
	nc.Subscribe(shared.CLUSTER_SYNC_SUBJECT, func(m *nats.Msg) {
		go func() {
			//log.Printf("Received a sync message: %s\n", string(m.Data))
			var s shared.DataEntry
			err := json.Unmarshal(m.Data, &s)
			shared.CheckError(err, "Unmarshal cluster sync")
			jibmem.sync(s.Key, s.Value, s.State)
		}()
	})
}

func handleStartupSyncRequest(nc *nats.Conn, jibmem *MDB) {
	nc.QueueSubscribe(shared.CLUSTER_SYNC_REQUEST_SUBJECT, shared.CLUSTER_SYNC_REQUEST_SUBJECT, func(m *nats.Msg) {
		log.Printf("Received a sync request message: %s\n", string(m.Data))
		jibmem.mem.Range(func(key, value interface{}) bool {
			v := value.(Record)
			output, err := json.Marshal(shared.DataEntry{Key: v.key, Value: v.value, State: v.state})
			shared.CheckError(err, "Marshal cluster startup-sync response")
			nc.Publish(m.Reply, output)
			return true
		})
	})
}

func handleStartupSync(nc *nats.Conn, jibmem *MDB) {
	syncChan := make(chan shared.DataEntry, 1000000)
	replyTo := "mizan.cluster.startup.sync." + uuid.New().String()

	subscription, err := nc.Subscribe(replyTo, func(m *nats.Msg) {
		go func() {
			var s shared.DataEntry
			err := json.Unmarshal(m.Data, &s)
			shared.CheckError(err, "Unmarshal cluster sync")
			syncChan <- s
		}()
	})
	shared.CheckError(err, "Registering startup sync subscription")
	nc.PublishRequest(shared.CLUSTER_SYNC_REQUEST_SUBJECT, replyTo, []byte{})

	counter := 0
ll:
	for {
		tk := time.NewTicker(3 * time.Second)
		select {
		case de := <-syncChan:
			jibmem.set(de.Key, de.Value)
			counter+=1
		case <-tk.C:
			break ll
		}
	}
	subscription.Unsubscribe()
	log.Printf("Synced %v keys", counter)
}
