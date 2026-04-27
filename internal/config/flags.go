package config

import "net/http"

type FeatureState struct {
	BypassCache bool
	DebugLog    bool
	// Add other flags here later
}

// ParseFlags extracts overrides from HTTP headers
func ParseFlags(r *http.Request) FeatureState {
	return FeatureState{
		BypassCache: r.Header.Get("X-Pisces-Flag-BypassCache") == "true",
		DebugLog:    r.Header.Get("X-Pisces-Flag-DebugLog") == "true",
	}
}
