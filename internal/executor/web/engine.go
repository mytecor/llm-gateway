// Package web is an alias/adapter for the unified external driver executor.
package web

import "github.com/mytecor/llm-gateway/internal/executor/external"

type Engine = external.DriverEngine

func New(command []string) (*Engine, error) {
	return external.NewDriver(external.Config{
		Protocol: external.ProtocolDriver,
		Command:  command,
	})
}
