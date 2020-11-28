package protostore

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	sync "sync"
	"time"

	"github.com/sebnyberg/protoio"
	"google.golang.org/protobuf/proto"
)

// PartitionedFileStore writes Protobuf messages to files, partitioned by a
// timestamp found in the protobuf message. The timestamp allows for range
// selection over the files.
type PartitionedFileAppender struct {
	baseDir          string
	keyFunc          func(proto.Message) string
	filePerm         os.FileMode
	fileAppenders    map[string]*ProtoAppender
	autoCloseTimeout time.Duration
}

type KeyFunc func(proto.Message) string

func NewPartitionedFileAppender(baseDir string, keyFunc KeyFunc, autoCloseTimeout time.Duration) *PartitionedFileAppender {
	return &PartitionedFileAppender{
		baseDir:          path.Clean(baseDir),
		keyFunc:          keyFunc,
		filePerm:         0644,
		fileAppenders:    make(map[string]*ProtoAppender, 1000),
		autoCloseTimeout: autoCloseTimeout,
	}
}

func (a *PartitionedFileAppender) AppendMessage(m proto.Message) error {
	k := a.keyFunc(m)
	_, exists := a.fileAppenders[k]
	if !exists {
		targetPath := path.Join(a.baseDir, k)
		w, err := NewProtoAppender(targetPath, a.autoCloseTimeout)
		if err != nil {
			return fmt.Errorf("failed to create proto appender: %v", err)
		}
		a.fileAppenders[k] = w
	}
	return a.fileAppenders[k].Append(m)
}

type ProtoAppender struct {
	path   string
	protoW *protoio.Writer

	// Auto-closing primitives
	closeAfterTime time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	err            error
	errMtx         sync.RWMutex
	reset          func()
}

func NewProtoAppender(path string, closeAfterTime time.Duration) (*ProtoAppender, error) {
	var a ProtoAppender
	a.path = path
	a.closeAfterTime = closeAfterTime

	// Initialize the proto writer and auto-closer
	if err := a.init(); err != nil {
		return nil, err
	}

	return &a, nil
}

// Initialize the proto writer and auto-closer.
func (a *ProtoAppender) init() error {
	a.errMtx.Lock()
	defer a.errMtx.Unlock()
	if a.err != nil {
		return a.err
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())
	err := os.MkdirAll(path.Dir(a.path), 0755)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	bufW := bufio.NewWriterSize(f, 1024*1024)
	a.protoW = protoio.NewWriter(bufW)
	timer := time.AfterFunc(a.closeAfterTime, func() {
		defer a.cancel()

		flushErr := bufW.Flush()
		closeErr := f.Close()

		a.errMtx.Lock()
		defer a.errMtx.Unlock()
		if flushErr != nil {
			a.err = flushErr
		}
		a.err = closeErr
	})
	a.reset = func() { timer.Reset(a.closeAfterTime) }
	return nil
}

func (a *ProtoAppender) Append(m proto.Message) error {
	// Re-initialize auto-closer if the underlying writers have been closed already
	select {
	case <-a.ctx.Done():
		// Initialize the proto writer and auto-closer
		if err := a.init(); err != nil {
			return err
		}
	default:
		// Reset timer (prolongue the open session)
		a.reset()
	}
	return a.protoW.WriteMsg(m)
}
