package main

import (
	"log"
	"time"
	"unsafe"

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
	ntotal            int = 1e7
	ndays             int = 1e4
	nperday           int = ntotal / ndays
)

func main() {
	localFS()
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
	}, time.Millisecond*10)

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
			check(appender.AppendMessage(&r))
		}
	}
}
