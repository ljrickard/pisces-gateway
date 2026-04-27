package config

import (
	"net/http"
	"strconv"

	"github.com/oklog/ulid/v2"
)

const (
	// The absolute default if no header is provided.
	fallbackHistoryLimit = 20
)

type FeatureState struct {
	BypassCache         bool
	ContextHistoryLimit int
}

type RequestMetadata struct {
	SessionID string
	Flags     FeatureState
}

func ParseRequestMetadata(r *http.Request) (RequestMetadata, bool) {
	// 1. Validate SessionID (ULID)
	sessionID := r.Header.Get("X-Pisces-Session-ID")
	parsedULID, err := ulid.Parse(sessionID)
	if err != nil {
		return RequestMetadata{}, false
	}

	// 2. Resolve History Limit (Header override or constant fallback)
	limit := fallbackHistoryLimit
	if headerVal := r.Header.Get("X-Pisces-Flag-ContextHistoryLimit"); headerVal != "" {
		if val, err := strconv.Atoi(headerVal); err == nil && val > 0 {
			limit = val
		}
	}

	return RequestMetadata{
		SessionID: parsedULID.String(),
		Flags: FeatureState{
			BypassCache:         r.Header.Get("X-Pisces-Flag-BypassCache") == "true",
			ContextHistoryLimit: limit,
		},
	}, true
}
