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
	Query        string
	SessionID    string
	History      []string
	Flags        config.FeatureState
	ReqConfig    map[string]any
	IsStream     bool
	StatusStream func(string)
	Attachments  []MultimodalPart
	Domain       string
	PendingTools []string
	FinalAnswer  string
	StreamBody   io.ReadCloser
	LoopCount    int
}
