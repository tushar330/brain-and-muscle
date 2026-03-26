package main

import (
	"encoding/binary"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	log.Println("Executing Attack 1: The OOM Bomb (Claiming a 2GB payload)")
	maliciousHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(maliciousHeader, 2000000000) // 2 GB
	conn.Write(maliciousHeader)

	// We don't even send the body. We wait to see if the Brain drops us.

	log.Println("Executing Attack 2: Garbage JSON")
	conn2, _ := net.Dial("tcp", "localhost:8080")
	garbage := []byte("{this is not valid json, crash please}")
	header2 := make([]byte, 4)
	binary.BigEndian.PutUint32(header2, uint32(len(garbage)))

	conn2.Write(append(header2, garbage...))
	log.Println("Attacks deployed. Check Brain logs.")
}
