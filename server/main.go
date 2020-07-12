package main

import (
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var NodeUUID = uuid.New().String()

const CLIENT_REQUEST_SUBJECT = "mizan.client.request"

const CLUSTER_REQUEST_SUBJECT = "mizan.cluster.request"
const CLUSTER_SYNC_SUBJECT = "mizan.cluster.sync"
const CLUSTER_SYNC_REQUEST_SUBJECT = "mizan.cluster.sync.request"



var clusterSet *nats.Subscription
var clusterGet *nats.Subscription
var clientSet *nats.Subscription
var clientGet *nats.Subscription

func main() {
	mizanDB := NewMDB()
	log.Print("DB initialized")

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL, nats.PingInterval(1*time.Second), nats.MaxPingsOutstanding(2))
	checkError(err, "Connecting to NATS")
	log.Print("Connected to NATS")

	log.Print("register handleClusterSync")
	handleClusterSync(nc, mizanDB)

	log.Print("About to sync with cluster")
	st:=time.Now()
	handleStartupSync(nc, mizanDB)
	log.Printf("Sync finished in %v \n", time.Since(st))

	log.Print("Start cluster handlers")
	clusterSet, err = handleClusterRequest(nc, mizanDB)
	checkError(err, "Register cluster set")

	handleStartupSyncRequest(nc, mizanDB)

	log.Print("Start client handlers")
	clientSet, err = handleClientRequest(nc)
	checkError(err, "Register client set")

	log.Print("Start completed.")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	signal.Notify(c, os.Interrupt, syscall.SIGKILL)
	<-c
	log.Print("Shutting down")
	nc.Drain()
	nc.Close()
}

func checkError(err error, desc string) {
	if err != nil {
		log.Fatalf("Error %v %v", desc, err)
	}
}
