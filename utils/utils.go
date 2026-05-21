package utils

import (
	"strings"
)

// CleanJSON strips markdown codeblock formatting from LLM responses
// to ensure they can be safely unmarshaled by json.Unmarshal.
func CleanJSON(raw string) string {
	clean := strings.TrimSpace(raw)
	clean = strings.TrimPrefix(clean, "```json")
	clean = strings.TrimPrefix(clean, "```")
	clean = strings.TrimSuffix(clean, "```")
	return strings.TrimSpace(clean)
}
