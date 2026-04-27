package pipeline

import (
	"log"
	"pisces-gateway/internal/config"
)

type Normalizer interface{ Process(string) string }
type Rewriter interface{ Resolve(string) string }
type Cache interface {
	Get(string) (string, bool)
	Set(string, string) error
}
type Intent interface{ Determine(string) string }
type Proxy interface {
	Forward(string, string, config.FeatureState) string
}

type Pipeline struct {
	Normalizer Normalizer
	Rewriter   Rewriter
	Cache      Cache
	Intent     Intent
	Proxy      Proxy
}

func (p *Pipeline) Execute(rawQuery string, flags config.FeatureState) string {
	log.Printf("🚀 Pipeline started for query: '%s'", rawQuery)

	clean := p.Normalizer.Process(rawQuery)
	rewritten := p.Rewriter.Resolve(clean)

	if !flags.BypassCache {
		if cached, hit := p.Cache.Get(rewritten); hit {
			return cached
		}
	} else {
		log.Println("⏭️  Cache bypassed via feature flag")
	}

	backend := p.Intent.Determine(rewritten)

	// Pass the flags downstream to the proxy!
	response := p.Proxy.Forward(backend, rewritten, flags)

	if !flags.BypassCache {
		p.Cache.Set(rewritten, response)
	}

	return response
}
