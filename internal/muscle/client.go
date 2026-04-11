package muscle

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

const (
	sendTimeout     = 5 * time.Second
	maxMessageBytes = 10 * 1024 * 1024 // 10 MB — must match brain
)

type ConnectionManager struct {
	peers  []string //  THE TOPOLOGY MAP
	nodeID string

	connMu sync.Mutex
	conn   net.Conn

	healthyMu sync.RWMutex
	healthy   chan struct{}

	pendingMu sync.Mutex
	pending   map[string]chan protocol.Message

	onConnect     func()
	onPushMessage func(protocol.Message)
}

func NewConnectionManager(peers []string, nodeID string) *ConnectionManager {
	return &ConnectionManager{
		peers:   peers,
		nodeID:  nodeID,
		healthy: make(chan struct{}), // starts closed — not yet connected
		pending: make(map[string]chan protocol.Message),
	}
}

func (cm *ConnectionManager) SetOnConnect(hook func()) {
	cm.onConnect = hook
}

func (cm *ConnectionManager) SetPushHandler(handler func(protocol.Message)) {
	cm.onPushMessage = handler
}

// Run establishes the initial connection and silently reconnects on failure.
func (cm *ConnectionManager) Run() {
	for {
		conn := cm.connectRoundRobin()

		log.Printf("[connmgr] connected to %s", conn.RemoteAddr().String())
		cm.setConn(conn)

		if cm.onConnect != nil {
			go cm.onConnect()
		}

		// readLoop blocks until the connection breaks.
		cm.readLoop(conn)

		log.Printf("[connmgr] connection lost — draining pending requests and reconnecting")
		cm.markUnhealthy()

		time.Sleep(2 * time.Second)
	}
}

// THE FAST-SWEEP DIALER
func (cm *ConnectionManager) connectRoundRobin() net.Conn {
	backoff := 500 * time.Millisecond

	for {
		// Snapshot the peers safely
		cm.connMu.Lock()
		currentPeers := append([]string(nil), cm.peers...)
		cm.connMu.Unlock()

		for _, targetAddr := range currentPeers {
			if targetAddr == "" {
				continue
			}

			// A 500ms timeout prevents the worker from hanging on dead IPs.
			conn, err := net.DialTimeout("tcp", targetAddr, 500*time.Millisecond)
			if err == nil {
				return conn
			}
		}

		// If we reach here, the entire cluster is currently unreachable.
		log.Printf("[connmgr] All cluster nodes unreachable. Backing off for %v", backoff)
		time.Sleep(backoff)

		// Cap the backoff at 2 seconds for aggressive sub-second recovery
		backoff *= 2
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
	}
}

func (cm *ConnectionManager) WaitUntilHealthy() {
	cm.healthyMu.RLock()
	ch := cm.healthy
	cm.healthyMu.RUnlock()
	<-ch
}

func (cm *ConnectionManager) SendRequest(opCode uint8, payload interface{}) (protocol.Message, error) {
	cm.WaitUntilHealthy()

	requestID := uuid.New().String()

	var rawPayload []byte
	if payload != nil {
		var err error
		rawPayload, err = json.Marshal(payload)
		if err != nil {
			return protocol.Message{}, fmt.Errorf("marshal payload: %w", err)
		}
	}

	frame := protocol.PackEnvelope(opCode, cm.nodeID, requestID, rawPayload)

	responseCh := make(chan protocol.Message, 1)
	cm.pendingMu.Lock()
	cm.pending[requestID] = responseCh
	cm.pendingMu.Unlock()

	cm.connMu.Lock()
	conn := cm.conn
	cm.connMu.Unlock()

	if conn == nil {
		cm.pendingMu.Lock()
		delete(cm.pending, requestID)
		cm.pendingMu.Unlock()
		return protocol.Message{}, fmt.Errorf("connection dropped before write for opcode 0x%X", opCode)
	}

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, writeErr := conn.Write(frame)

	if writeErr != nil {
		cm.pendingMu.Lock()
		delete(cm.pending, requestID)
		cm.pendingMu.Unlock()

		cm.connMu.Lock()
		cm.conn = nil
		cm.connMu.Unlock()

		conn.Close()
		return protocol.Message{}, fmt.Errorf("write failed: %w", writeErr)
	}

	select {
	case resp, ok := <-responseCh:
		if !ok {
			return protocol.Message{}, fmt.Errorf("connection lost while waiting for %d response", opCode)
		}

		if resp.OpCode == protocol.OpRedirect {
			var redirect protocol.RedirectPayload
			json.Unmarshal(resp.Payload, &redirect)

			newAddr := redirect.LeaderID
			if newAddr != "" && !strings.HasSuffix(newAddr, ":8080") {
				newAddr += ":8080"
			}

			log.Printf("[connmgr] Redirected by Follower. Switching target to Leader: %s", newAddr)

			//  DYNAMIC TOPOLOGY SHIFT
			// Move the new Leader to the very front of the peers array
			cm.connMu.Lock()
			newPeers := []string{newAddr}
			for _, p := range cm.peers {
				if p != newAddr && p != "" {
					newPeers = append(newPeers, p)
				}
			}
			cm.peers = newPeers
			cm.connMu.Unlock()

			cm.markUnhealthy()

			return protocol.Message{}, fmt.Errorf("redirected to %s", newAddr)
		}

		return resp, nil

	case <-time.After(sendTimeout):
		cm.pendingMu.Lock()
		delete(cm.pending, requestID)
		cm.pendingMu.Unlock()
		return protocol.Message{}, fmt.Errorf("request %d (%s) timed out after %v", opCode, requestID, sendTimeout)
	}
}

// GetActiveHost returns the hostname or IP of the Brain currently connected to.
// This is used to inject the correct control plane address into Docker tasks.
func (cm *ConnectionManager) GetActiveHost() string {
	cm.connMu.Lock()
	defer cm.connMu.Unlock()

	if cm.conn == nil {
		return "localhost"
	}

	remoteAddr := cm.conn.RemoteAddr().String()
	// Strip the port (e.g., "1.2.3.4:9000" -> "1.2.3.4")
	idx := strings.LastIndex(remoteAddr, ":")
	if idx != -1 {
		return remoteAddr[:idx]
	}
	return remoteAddr
}

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

		nodeID, reqID, _, payloadBytes, err := protocol.UnpackEnvelope(body)

		if err != nil {
			log.Printf("[readloop] unpack error: %v — skipping frame", err)
			continue
		}

		msg := protocol.Message{
			OpCode:    header[4],
			NodeID:    nodeID,
			RequestID: reqID,
			Payload:   payloadBytes,
		}

		cm.pendingMu.Lock()
		ch, found := cm.pending[msg.RequestID]
		if found {
			delete(cm.pending, msg.RequestID)
		}
		cm.pendingMu.Unlock()

		if found {
			ch <- msg
		} else {
			if cm.onPushMessage != nil {
				go cm.onPushMessage(msg)
			} else {
				log.Printf("[readloop] dropped orphaned message %q (no push handler)", msg.RequestID)
			}
		}
	}
}

func (cm *ConnectionManager) setConn(conn net.Conn) {
	cm.connMu.Lock()
	cm.conn = conn
	cm.connMu.Unlock()

	cm.healthyMu.Lock()
	close(cm.healthy)
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
	select {
	case <-cm.healthy:
		cm.healthy = make(chan struct{})
	default:
	}
	cm.healthyMu.Unlock()

	cm.pendingMu.Lock()
	for id, ch := range cm.pending {
		close(ch)
		delete(cm.pending, id)
	}
	cm.pendingMu.Unlock()
}
