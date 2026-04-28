package config

import (
	"net/http"
	"strconv"
	"strings"
)

type FeatureState struct {
	SkipCache           bool
	SimilarityThreshold float64
	ContextHistoryLimit int
}

type RequestMetadata struct {
	SessionID string
	Flags     FeatureState
}

func ParseRequestMetadata(r *http.Request) (RequestMetadata, bool) {
	sessionID := r.Header.Get("X-Pisces-Session-ID")
	if sessionID == "" {
		return RequestMetadata{}, false
	}

	flags := FeatureState{
		SkipCache:           false,
		SimilarityThreshold: 0.90,
		ContextHistoryLimit: 20,
	}

	if skip := r.Header.Get("X-Pisces-Flag-SkipCache"); skip != "" {
		flags.SkipCache = strings.ToLower(skip) == "true"
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
