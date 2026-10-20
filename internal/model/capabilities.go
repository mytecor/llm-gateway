package model

// Capabilities describe behavior, not a transport. Unknown capabilities are
// never silently emulated by the scheduler.
type Capabilities struct {
	Chat, Responses, Streaming, Tools  bool
	Stateful, Resume, Fork, SafeReplay bool
}

// ModelInfo is optional executor discovery metadata; IDs remain native here.
type ModelInfo struct {
	ID, Name         string
	ReasoningEfforts []string
}
