package main

import (
	"github.com/MohammadTaheri304/mizan/shared"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var NodeUUID = uuid.New().String()

func main() {
	mizanDB := NewMDB()
	log.Print("DB initialized")

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL, nats.PingInterval(1*time.Second), nats.MaxPingsOutstanding(2))
	shared.CheckError(err, "Connecting to NATS")
	log.Print("Connected to NATS")

	log.Print("register handleClusterSync")
	handleClusterSync(nc, mizanDB)

	log.Print("About to sync with cluster")
	st := time.Now()
	handleStartupSync(nc, mizanDB)
	log.Printf("Sync finished in %v \n", time.Since(st))

	log.Print("Start cluster handlers")
	_, err = handleClusterRequest(nc, mizanDB)
	shared.CheckError(err, "Register cluster set")

	handleStartupSyncRequest(nc, mizanDB)

	log.Print("Start client handlers")
	_, err = handleClientRequest(nc)
	shared.CheckError(err, "Register client set")

	log.Print("Start completed.")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	signal.Notify(c, os.Interrupt, syscall.SIGKILL)
	<-c
	log.Print("Shutting down")
	nc.Drain()
	nc.Close()
}
