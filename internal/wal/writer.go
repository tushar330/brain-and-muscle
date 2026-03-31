package wal

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
)

type EventType uint8

const (
	EventTaskQueued     EventType = iota // SetTask: new task entered the store
	EventTaskDispatched                  // TransitionTaskState Pending → Running
	EventTaskReverted                    // TransitionTaskState Running → Pending (TCP send failed)
	EventTaskCompleted                   // CompleteTask: finished successfully
	EventTaskFailed                      // HandleMAPFailure: exhausted retries or Version A fail
	EventJobRegistered                   // RegisterJob: SCATTER accepted, all MAP tasks + GATHER pre-staged
	EventMAPCompleted                    // RecordMAPComplete: one MAP chunk finished
	EventMAPFailed                       // RecordMAPFailed: one MAP chunk exhausted all retries
)

type LogEntry struct {
	LSN      uint64    `json:"lsn"`
	UnixNano int64     `json:"ts"`
	Event    EventType `json:"event"`
	Term     uint64    `json:"term,omitempty"`
	TaskID   string    `json:"task_id,omitempty"`
	JobID    string    `json:"job_id,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	Payload  []byte    `json:"payload,omitempty"`
}

type WriteRequest struct {
	IsSnapshot   bool
	SnapshotData []byte
	Entry        LogEntry
	Conn         net.Conn
	OpCode       uint8
	RequestID    string
	Payload      interface{}
}

type GroupCommitWAL struct {
	path     string
	snapPath string
	file     *os.File
	writer   *bufio.Writer
	walInbox chan WriteRequest
	lsn      uint64
}

func NewGroupCommitWAL(walPath string, snapPath string) (*GroupCommitWAL, error) {
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	w := &GroupCommitWAL{
		path:     walPath,
		snapPath: snapPath,
		file:     file,
		writer:   bufio.NewWriterSize(file, 4*1024*1024),
		walInbox: make(chan WriteRequest, 10000),
	}

	go w.CommitLoop()
	return w, nil
}

func (w *GroupCommitWAL) Enqueue(req WriteRequest) {
	w.walInbox <- req
}

func (w *GroupCommitWAL) CommitLoop() {
	log.Println("[wal] Group Commit engine started (10ms batching)")

	ticker := time.NewTicker(10 * time.Millisecond)
	var batch []WriteRequest

	for {
		select {
		case req := <-w.walInbox:

			if req.IsSnapshot {
				if len(batch) > 0 {
					w.flushAndAck(&batch)
				}

				// 🚨 Use the dynamic node-specific paths!
				tmpFile := w.snapPath + ".tmp"
				if err := os.WriteFile(tmpFile, req.SnapshotData, 0644); err != nil {
					log.Fatalf("[wal] CRITICAL: failed to write snapshot to %s: %v", tmpFile, err)
				}
				if err := os.Rename(tmpFile, w.snapPath); err != nil {
					log.Fatalf("[wal] CRITICAL: failed to rename snapshot: %v", err)
				}

				w.writer.Flush()
				w.file.Close()

				newFile, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					log.Fatalf("[wal] catastrophic failure rotating WAL: %v", err)
				}
				w.file = newFile
				w.writer.Reset(w.file)

				log.Printf("[wal] compacted into %s and wiped %s", w.snapPath, w.path)
				continue
			}

			// Normal WAL append.
			req.Entry.LSN = atomic.AddUint64(&w.lsn, 1)
			req.Entry.UnixNano = time.Now().UnixNano()

			line, _ := json.Marshal(req.Entry)
			line = append(line, '\n')
			if _, err := w.writer.Write(line); err != nil {
				log.Fatalf("[wal] CRITICAL: disk write failed: %v", err)
			}

			batch = append(batch, req)

			if len(batch) >= 1000 {
				w.flushAndAck(&batch)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				w.flushAndAck(&batch)
			}
		}
	}
}

// flushAndAck syncs the WAL to disk and then ACKs every pending connection in
// the batch.
func (w *GroupCommitWAL) flushAndAck(batch *[]WriteRequest) {
	w.writer.Flush()
	w.file.Sync()

	ackBatch := make([]WriteRequest, len(*batch))
	copy(ackBatch, *batch)

	go func(reqs []WriteRequest) {
		for _, req := range reqs {
			protocol.SendResponse(req.Conn, req.OpCode, req.RequestID, req.Payload)
		}
	}(ackBatch)

	*batch = (*batch)[:0]
}

// ReadAll scans the WAL file and returns all valid entries in LSN order.
func (w *GroupCommitWAL) ReadAll() ([]LogEntry, error) {
	readF, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer readF.Close()

	const maxLineBytes = 4 * 1024 * 1024
	buf := make([]byte, maxLineBytes)
	scanner := bufio.NewScanner(readF)
	scanner.Buffer(buf, maxLineBytes)

	var entries []LogEntry
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("[wal] corrupt entry after LSN %d — stopping replay (crash boundary)",
				func() uint64 {
					if len(entries) > 0 {
						return entries[len(entries)-1].LSN
					}
					return 0
				}())
			break
		}
		entries = append(entries, entry)
	}

	return entries, scanner.Err()
}
