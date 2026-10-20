package main

import (
	"log"
	"os"

	"github.com/mytecor/llm-gateway/internal/gateway"
)

func main() {
	if err := gateway.Run(os.Args[1:]); err != nil {
		log.Printf("llm-gateway: %v", err)
		os.Exit(1)
	}
}
