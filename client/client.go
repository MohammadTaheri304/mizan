package main

import (
	"bufio"
	"encoding/json"
	"github.com/MohammadTaheri304/mizan/shared"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)


func main() {
	main_cli()
	//main_test_set()
	//main_test_get()
	//main_test_normal()
}

func main_test_normal() {

	// Connect to a server
	nc, err := nats.Connect(nats.DefaultURL)
	shared.CheckError(err, "")
	log.Print("Connected to NATS")

	for {
		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			// set a random key
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

			// unset a random key
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()

				res, err := unsetServer(nc, strconv.Itoa(int(uuid.New().ID()%10000)))
				if err != nil {
					log.Println("Error unset ", err)
				} else if res.Confidence < 1 {
					log.Println("loosly unset answer ", res)
				}

			}(&wg)

			// get a random key
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()

				res, err := getFormServer(nc, strconv.Itoa(int(uuid.New().ID()%10000)))
				if err != nil {
					log.Println("Error get ", err)
					return
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
	shared.CheckError(err, "")
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
	shared.CheckError(err, "")
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
	shared.CheckError(err, "")
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
		} else if args[0] == "unset" {
			res, err := unsetServer(nc, args[1])
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

func getFormServer(nc *nats.Conn, key string) (*shared.DataEntry, error) {
	request, err := json.Marshal(shared.DataEntry{OpCode: "GET", Key: key})
	if err != nil {
		return nil, err
	}

	m, err := nc.Request(shared.CLIENT_REQUEST_SUBJECT, request, 5*time.Second)
	if err != nil {
		return nil, err
	}

	var s shared.DataEntry
	err = json.Unmarshal(m.Data, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func setToServer(nc *nats.Conn, key, value string) (*shared.DataEntry, error) {
	request, err := json.Marshal(shared.DataEntry{OpCode: "SET", Key: key, Value: value})
	if err != nil {
		return nil, err
	}

	m, err := nc.Request(shared.CLIENT_REQUEST_SUBJECT, request, 5*time.Second)
	if err != nil {
		return nil, err
	}

	var s shared.DataEntry
	err = json.Unmarshal(m.Data, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func unsetServer(nc *nats.Conn, key string) (*shared.DataEntry, error) {
	request, err := json.Marshal(shared.DataEntry{OpCode: "UNSET", Key: key})
	if err != nil {
		return nil, err
	}

	m, err := nc.Request(shared.CLIENT_REQUEST_SUBJECT, request, 5*time.Second)
	if err != nil {
		return nil, err
	}

	var s shared.DataEntry
	err = json.Unmarshal(m.Data, &s)
	if err != nil {
		return nil, err
	}

	return &s, nil
}
