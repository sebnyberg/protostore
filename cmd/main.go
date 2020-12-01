package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v2"
	"github.com/sebnyberg/protostore"
	"github.com/sebnyberg/protostore/examplepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

const (
	defaultLocalPath      = ".local"
	defaultBadgerPath     = ".badger"
	ntotal            int = 1e6
	ndays             int = 1e3
	nperday           int = ntotal / ndays
)

func main() {
	os.RemoveAll(defaultLocalPath)
	os.RemoveAll(defaultBadgerPath)
	badger0()
	// localFS()
	// badger1()
}

func badger0() {
	db, err := badger.Open(badger.DefaultOptions(defaultBadgerPath))
	check(err)
	defer db.Close()
}

func badger1() {
	db, err := badger.Open(badger.DefaultOptions(defaultBadgerPath))
	check(err)
	defer db.Close()

	r := examplepb.Report{
		ReportComment:    "aaaaaaaaaaaaaaaaaa",
		ReportId:         "bbbbbbbbbbbbbbbb",
		ReportedByUserId: "ccccccccccccccc",
		UpdatedAt:        timestamppb.New(time.Now()),
		RegisteredAt:     timestamppb.New(time.Now()),
		EventTs:          timestamppb.New(time.Now().AddDate(0, 0, -ndays)),
	}

	tsBuf := make([]byte, 0, 10)
	key := func(r *examplepb.Report) []byte {
		// return ksuid.New().Bytes()
		tsBuf = tsBuf[:0]
		tsBuf = r.EventTs.AsTime().AppendFormat(tsBuf, "2006/01/02")
		return tsBuf[:]
	}
	lenBuf := make([]byte, 8)
	msgBuf := make([]byte, 32)
	val := func(r *examplepb.Report) []byte {
		msgLen := proto.Size(r)
		binary.BigEndian.PutUint64(lenBuf, uint64(msgLen))
		if cap(msgBuf) < msgLen {
			msgBuf = make([]byte, msgLen)
		}
		msgBytes, err := proto.MarshalOptions{}.MarshalAppend(msgBuf[:0], r)
		check(err)
		return msgBytes
	}

	txn := db.NewTransaction(true)
	defer func(start time.Time) {
		fmt.Printf("Execution took %s\n", time.Since(start))
	}(time.Now())
	for i := 0; i < ntotal; i++ {
		if i%1e5 == 0 {
			fmt.Println(i)
		}
		err := txn.Set(key(&r), val(&r))
		if err != nil {
			if err != badger.ErrTxnTooBig {
				check(err)
			}
			check(txn.Commit())
			txn = db.NewTransaction(true)
			check(txn.Set(key(&r), val(&r)))
		}
	}
}

func localFS() {
	defer func(start time.Time) {
		log.Printf("Execution took %v\n", time.Since(start))
	}(time.Now())
	tsBuf := make([]byte, 0, 10)
	appender := protostore.NewPartitionedFileAppender(defaultLocalPath, func(m proto.Message) string {
		if r, ok := m.(*examplepb.Report); ok {
			tsBuf = tsBuf[:0]
			tsBuf = r.EventTs.AsTime().AppendFormat(tsBuf, "2006/01/02")
			return *(*string)(unsafe.Pointer(&tsBuf))
		}
		return "out"
	}, time.Millisecond*1000)

	r := examplepb.Report{
		ReportComment:    "aaaaaaaaaaaaaaaaaa",
		ReportId:         "bbbbbbbbbbbbbbbb",
		ReportedByUserId: "ccccccccccccccc",
		UpdatedAt:        timestamppb.New(time.Now()),
		RegisteredAt:     timestamppb.New(time.Now()),
		EventTs:          timestamppb.New(time.Now().AddDate(0, 0, -ndays)),
	}

	for i := 0; i < ntotal; i++ {
		if i%nperday == 0 {
			r.EventTs = timestamppb.New(r.EventTs.AsTime().AddDate(0, 0, 1))
		}
		check(appender.AppendMessage(&r))
	}
}
