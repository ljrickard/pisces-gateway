package config

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/oklog/ulid/v2"
)

type FeatureState struct {
	SkipCache           bool
	NoSession           bool
	SimilarityThreshold float64
	ContextHistoryLimit int
}

type RequestMetadata struct {
	SessionID string
	Flags     FeatureState
}

func ParseRequestMetadata(r *http.Request) (RequestMetadata, bool) {
	sessionID := r.Header.Get("X-Pisces-Session-ID")

	// Default flags
	flags := FeatureState{
		SkipCache:           false,
		NoSession:           false,
		SimilarityThreshold: 0.90, // Standard default
		ContextHistoryLimit: 20,
	}

	// Capture flags even if sessionID is missing
	if skip := r.Header.Get("X-Pisces-Flag-SkipCache"); skip != "" {
		flags.SkipCache = strings.ToLower(skip) == "true"
	}
	if noSession := r.Header.Get("X-Pisces-Flag-NoSession"); noSession != "" {
		flags.NoSession = strings.ToLower(noSession) == "true"
	}
	if thresholdStr := r.Header.Get("X-Pisces-Similarity-Threshold"); thresholdStr != "" {
		parsed, err := strconv.ParseFloat(thresholdStr, 64)
		if err == nil && parsed >= 0.0 && parsed <= 1.0 {
			flags.SimilarityThreshold = parsed
		}
	}

	// Validate SessionID only if it's provided
	if sessionID != "" {
		if _, err := ulid.Parse(sessionID); err != nil {
			return RequestMetadata{Flags: flags}, false // Invalid ID, but keep flags
		}
	}

	return RequestMetadata{
		SessionID: sessionID,
		Flags:     flags,
	}, true
}
