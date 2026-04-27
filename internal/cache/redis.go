package cache

import "log"

// RedisClient is the concrete struct
type RedisClient struct{}

func NewRedisClient() *RedisClient {
	return &RedisClient{}
}

// Implicitly implements pipeline.Cache
func (r *RedisClient) Get(key string) (string, bool) {
	log.Printf("🔍 Checking cache for: %s", key)
	return "", false // Force a miss for testing
}

func (r *RedisClient) Set(key, val string) error {
	log.Printf("💾 Saving to cache: %s", key)
	return nil
}
