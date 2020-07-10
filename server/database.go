package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"sync"
)

type MDB struct {
	mem    sync.Map
	locker *GoLocker
}

type Record struct {
	key   string
	value string
	state string // md5 hash
}

func NewMDB() *MDB {
	return &MDB{mem: sync.Map{}, locker: NewGoLocker()}
}

func (jm *MDB) set(key, value string) (Record, error) {
	lr := jm.locker.Lock(key)
	if lr {
		defer jm.locker.Unlock(key)
		record, _ := jm.get(key)
		record.value = value
		record.state = getMD5Hash(value)
		jm.mem.Store(key, record)
		//log.Printf("Update key %v state to %v \n", key, record.state)
		return record, nil
	} else {
		return Record{}, errors.New("unable.to.lock.key")
	}
}

func (jm *MDB) sync(key, value, state string) (Record,error) {
	lr := jm.locker.Lock(key)
	if lr {
		defer jm.locker.Unlock(key)
		record := Record{key: key, value: value, state: state}
		jm.mem.Store(record.key, record)
		//log.Printf("Sync record %v \n", record)
		return record, nil
	} else {
		return Record{}, errors.New("unable.to.lock.key")
	}
}

func (jm *MDB) get(key string) (Record, bool) {
	res, ok := jm.mem.Load(key)
	if ok {
		return res.(Record), ok
	}
	return Record{key: key, state: ""}, ok
}

func getMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
