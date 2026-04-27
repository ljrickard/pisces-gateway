package normalize

import "log"

type Service struct{}

func NewService() *Service { return &Service{} }

func (s *Service) Process(query string) string {
	log.Println("🧹 Normalizing query...")
	return query
}
