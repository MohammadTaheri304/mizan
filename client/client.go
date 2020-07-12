package main

import (
	"bufio"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const CLIENT_REQUEST_SUBJECT = "mizan.client.request"

func main() {
	//main_cli()
	//main_test_set()
	//main_test_get()
	main_test_normal()
}

func main_test_normal() {

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL)
	checkError(err)
	log.Print("Connected to NATS")

	for {
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()

				res, err := setToServer(nc, strconv.Itoa(int(uuid.New().ID()%10000)), uuid.New().String())
				if err != nil {
					log.Println("Error set ", err)
				} else if res.Confidence < 1 {
					log.Println("loosly set answer ", res)
				}

			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()

				res, err := getFormServer(nc, strconv.Itoa(int(uuid.New().ID()%10000)))
				if err != nil {
					log.Println("Error get ", err)
				} else if res.Confidence < 1 {
					log.Println("loosly get answer ", res)
				}

			}(&wg)
		}
		wg.Wait()
		log.Println(time.Now().Unix())
	}
}

func main_test_get() {

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL)
	checkError(err)
	log.Print("Connected to NATS")

	for {
		wg := sync.WaitGroup{}
		for i := 0; i < 1000; i++ {
			//<-tk.C
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()

				res, err := getFormServer(nc, strconv.Itoa(int(uuid.New().ID()%1000000)))
				if err != nil {
					log.Println("Error get %v", err)
				} else if res.Confidence < 1 {
					log.Println("loosly get answer %v", res)
				}
				//log.Print(res)
			}(&wg)
		}
		wg.Wait()
		log.Println(time.Now().Unix())
	}
}

func main_test_set() {

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL)
	checkError(err)
	log.Print("Connected to NATS")

	for {
		wg := sync.WaitGroup{}
		//tk := time.NewTicker(100 * time.Millisecond)
		for i := 0; i < 1000; i++ {
			//<-tk.C
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()

				res, err := setToServer(nc, strconv.Itoa(int(uuid.New().ID()%1000000)), uuid.New().String())
				if err != nil {
					log.Println("Error set ", err)
				} else if res.Confidence < 1 {
					log.Println("loosly set answer ", res)
				}
				//log.Print(res)
			}(&wg)
		}
		wg.Wait()
		log.Println(time.Now().Unix())
	}
}

func main_cli() {

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL)
	checkError(err)
	log.Print("Connected to NATS")

	reader := bufio.NewReader(os.Stdin)
	for true {
		text, _ := reader.ReadString('\n')
		args := strings.Fields(text)

		if args[0] == "set" {
			res, err := setToServer(nc, args[1], args[2])
			if err != nil {
				log.Fatal("Error %v", err)
			}
			log.Print(res)
		} else if args[0] == "get" {
			res, err := getFormServer(nc, args[1])
			if err != nil {
				log.Fatal("Error %v", err)
			}
			log.Print(res)
		}
	}
}

func getFormServer(nc *nats.Conn, key string) (*DataEntry, error) {
	request, err := json.Marshal(DataEntry{OpCode: "GET", Key: key})
	if err != nil {
		return nil, err
	}

	m, err := nc.Request(CLIENT_REQUEST_SUBJECT, request, 2*time.Second)
	if err != nil {
		return nil, err
	}

	var s DataEntry
	err = json.Unmarshal(m.Data, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func setToServer(nc *nats.Conn, key, value string) (*DataEntry, error) {
	request, err := json.Marshal(DataEntry{OpCode: "SET", Key: key, Value: value})
	if err != nil {
		return nil, err
	}

	m, err := nc.Request(CLIENT_REQUEST_SUBJECT, request, 2*time.Second)
	if err != nil {
		return nil, err
	}

	var s DataEntry
	err = json.Unmarshal(m.Data, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

type DataEntry struct {
	OpCode     string  `json:"opcode"`
	Key        string  `json:"key"`
	Value      string  `json:"value"`
	State      string  `json:"state"`
	Confidence float32 `json:"confidence"`
}

func checkError(err error) {
	if err != nil {
		log.Fatal("Error %v", err)
	}
}
