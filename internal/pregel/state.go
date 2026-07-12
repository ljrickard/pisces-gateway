package pregel

import (
	"io"
	"pisces-gateway/internal/config"
)

type MultimodalPart struct {
	MimeType string
	Data     []byte
}

type SubTask struct {
	Query  string
	Domain string // "frasier" or "generic"
	Answer string
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
	Tasks        []SubTask
	FinalAnswer  string
	StreamBody   io.ReadCloser
	LoopCount    int
	IsCacheHit   bool
	HasError     bool
}
