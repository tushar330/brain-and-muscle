package muscle

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

const (
	dialTimeout     = 5 * time.Second
	sendTimeout     = 5 * time.Second
	maxRetries      = 10
	baseDelay       = 500 * time.Millisecond
	maxDelay        = 30 * time.Second
	maxMessageBytes = 10 * 1024 * 1024 // 10 MB — must match brain
)

type ConnectionManager struct {
	addr   string
	nodeID string

	connMu sync.Mutex
	conn   net.Conn

	healthyMu sync.RWMutex
	healthy   chan struct{}

	pendingMu sync.Mutex
	pending   map[string]chan protocol.Message

	onConnect func()
}

func NewConnectionManager(addr, nodeID string) *ConnectionManager {
	return &ConnectionManager{
		addr:    addr,
		nodeID:  nodeID,
		healthy: make(chan struct{}), // starts closed — not yet connected
		pending: make(map[string]chan protocol.Message),
	}
}

func (cm *ConnectionManager) SetOnConnect(hook func()) {
	cm.onConnect = hook
}

// Run establishes the initial connection and silently reconnects on failure.
func (cm *ConnectionManager) Run() {
	for {
		log.Printf("[connmgr] connecting to brain at %s", cm.addr)

		conn, err := connectWithBackoff(cm.addr)
		if err != nil {
			log.Fatalf("[connmgr] permanent failure: %v", err)
		}

		log.Printf("[connmgr] connected")
		cm.setConn(conn)

		if cm.onConnect != nil {
			go cm.onConnect()
		}

		// readLoop blocks until the connection breaks.
		cm.readLoop(conn)

		log.Printf("[connmgr] connection lost — draining pending requests and reconnecting")
		cm.markUnhealthy()
	}
}

// WaitUntilHealthy blocks the caller until a live connection is available.
func (cm *ConnectionManager) WaitUntilHealthy() {
	cm.healthyMu.RLock()
	ch := cm.healthy
	cm.healthyMu.RUnlock()
	<-ch
}

func (cm *ConnectionManager) SendRequest(opCode uint8, payload interface{}) (protocol.Message, error) {
	cm.WaitUntilHealthy()

	requestID := uuid.New().String()

	msg := protocol.Message{
		NodeID:    cm.nodeID,
		RequestID: requestID,
		Timestamp: time.Now(),
	}

	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return protocol.Message{}, fmt.Errorf("marshal payload: %w", err)
		}
		msg.Payload = raw
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return protocol.Message{}, fmt.Errorf("marshal message: %w", err)
	}

	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(body)))
	header[4] = opCode
	frame := append(header, body...)

	responseCh := make(chan protocol.Message, 1)
	cm.pendingMu.Lock()
	cm.pending[requestID] = responseCh
	cm.pendingMu.Unlock()

	cm.connMu.Lock()
	conn := cm.conn
	if conn == nil {
		cm.connMu.Unlock()
		cm.pendingMu.Lock()
		delete(cm.pending, requestID)
		cm.pendingMu.Unlock()
		return protocol.Message{}, fmt.Errorf("connection dropped before write for opcode 0x%X", opCode)
	}

	_, writeErr := conn.Write(frame)
	if writeErr != nil {
		// sees nil and skips its own Close() on the same underlying connection.
		cm.conn = nil
	}
	cm.connMu.Unlock()

	if writeErr != nil {
		cm.pendingMu.Lock()
		delete(cm.pending, requestID)
		cm.pendingMu.Unlock()

		conn.Close() // close the local copy; markUnhealthy will skip it
		return protocol.Message{}, fmt.Errorf("write failed: %w", writeErr)
	}

	select {
	case resp, ok := <-responseCh:
		if !ok {
			// markUnhealthy closed the channel — fast-fail path.
			return protocol.Message{}, fmt.Errorf("connection lost while waiting for %d response", opCode)
		}
		return resp, nil

	case <-time.After(sendTimeout):
		cm.pendingMu.Lock()
		delete(cm.pending, requestID)
		cm.pendingMu.Unlock()
		return protocol.Message{}, fmt.Errorf("request %d (%s) timed out after %v", opCode, requestID, sendTimeout)
	}
}

// readLoop is the ONLY goroutine that reads from the socket.
func (cm *ConnectionManager) readLoop(conn net.Conn) {
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			log.Printf("[readloop] header read error: %v — exiting", err)
			return
		}

		bodyLen := binary.BigEndian.Uint32(header[0:4])
		if bodyLen > maxMessageBytes {
			log.Printf("[readloop] oversized body: %d bytes — exiting", bodyLen)
			return
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			log.Printf("[readloop] body read error: %v — exiting", err)
			return
		}

		var msg protocol.Message
		if err := json.Unmarshal(body, &msg); err != nil {
			log.Printf("[readloop] unmarshal error: %v — skipping frame", err)
			continue
		}

		msg.OpCode = header[4]

		cm.pendingMu.Lock()
		ch, found := cm.pending[msg.RequestID]
		if found {
			delete(cm.pending, msg.RequestID)
		}
		cm.pendingMu.Unlock()

		if found {
			ch <- msg
		} else {
			log.Printf("[readloop] no pending request for RequestID %q", msg.RequestID)
		}
	}
}

func (cm *ConnectionManager) setConn(conn net.Conn) {
	cm.connMu.Lock()
	cm.conn = conn
	cm.connMu.Unlock()

	cm.healthyMu.Lock()
	close(cm.healthy) // unblocks all goroutines waiting in WaitUntilHealthy
	cm.healthyMu.Unlock()
}

func (cm *ConnectionManager) markUnhealthy() {
	cm.connMu.Lock()
	if cm.conn != nil {
		cm.conn.Close()
		cm.conn = nil
	}
	cm.connMu.Unlock()

	cm.healthyMu.Lock()
	cm.healthy = make(chan struct{}) // worker loops will block on next WaitUntilHealthy
	cm.healthyMu.Unlock()

	cm.pendingMu.Lock()
	for id, ch := range cm.pending {
		close(ch)
		delete(cm.pending, id)
	}
	cm.pendingMu.Unlock()
}

func connectWithBackoff(addr string) (net.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err == nil {
			return conn, nil
		}
		lastErr = err

		if attempt == maxRetries-1 {
			break // no sleep on the last attempt
		}

		delay := baseDelay * (1 << attempt)
		if delay > maxDelay {
			delay = maxDelay
		}
		jitter := 0.5 + rand.Float64()
		sleep := time.Duration(float64(delay) * jitter)

		log.Printf("[dial] attempt %d/%d failed: %v — retrying in %v",
			attempt+1, maxRetries, err, sleep.Round(time.Millisecond))

		time.Sleep(sleep)
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}
