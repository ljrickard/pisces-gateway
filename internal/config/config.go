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

	// --- NEW: Strict ULID Edge Validation ---
	if _, err := ulid.Parse(sessionID); err != nil {
		// If it's empty or malformed, instantly reject the request
		return RequestMetadata{}, false
	}

	flags := FeatureState{
		SkipCache:           false,
		NoSession:           false,
		SimilarityThreshold: 0.90,
		ContextHistoryLimit: 20,
	}

	if skip := r.Header.Get("X-Pisces-Flag-SkipCache"); skip != "" {
		flags.SkipCache = strings.ToLower(skip) == "true"
	}

	// --- NEW: Parse the NoSession flag ---
	if noSession := r.Header.Get("X-Pisces-Flag-NoSession"); noSession != "" {
		flags.NoSession = strings.ToLower(noSession) == "true"
	}

	if thresholdStr := r.Header.Get("X-Pisces-Similarity-Threshold"); thresholdStr != "" {
		parsed, err := strconv.ParseFloat(thresholdStr, 64)
		if err == nil && parsed >= 0.0 && parsed <= 1.0 {
			flags.SimilarityThreshold = parsed
		}
	}

	if headerVal := r.Header.Get("X-Pisces-Flag-ContextHistoryLimit"); headerVal != "" {
		if val, err := strconv.Atoi(headerVal); err == nil && val > 0 {
			flags.ContextHistoryLimit = val
		}
	}

	return RequestMetadata{
		SessionID: sessionID,
		Flags:     flags,
	}, true
}
