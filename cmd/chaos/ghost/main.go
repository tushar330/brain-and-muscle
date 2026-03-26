package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"net"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	req := protocol.SubmitTaskPayload{QueueName: "default", Payload: "ghost-task"}
	rawReq, _ := json.Marshal(req)

	msg := protocol.Message{
		OpCode:    protocol.OpSubmitTask,
		RequestID: uuid.New().String(),
		Payload:   rawReq,
	}

	body, _ := json.Marshal(msg)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(body)))

	log.Println("Sending valid request...")
	conn.Write(append(header, body...))

	log.Println("Instantly severing connection before Brain can reply!")
	conn.Close() // Simulate a hard crash on the client side
}
