package pregel

import (
	"io"
	"pisces-gateway/internal/config"
)

type MultimodalPart struct {
	MimeType string
	Data     []byte
}
type AgentState struct {
	// Core Request Data
	Query        string
	History      []string
	Config       config.FeatureState
	IsStream     bool // <-- ADD THIS: Does the user want SSE?
	StatusStream func(string)

	// Future-proofing for Vision/OCR Agents
	Attachments []MultimodalPart

	// Graph Memory & Context
	Domain         string
	SearchContexts []string
	PendingTools   []string

	// Output & Safety
	FinalAnswer string
	StreamBody  io.ReadCloser // <-- ADD THIS: The open socket from the downstream bot
	LoopCount   int
}
