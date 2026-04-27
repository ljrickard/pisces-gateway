package normalize

import (
	"context"
	"strings"
)

type Service struct{}

func NewService() *Service { return &Service{} }

func (s *Service) Process(ctx context.Context, query string) string {
	return strings.TrimSpace(strings.ToLower(query))
}
