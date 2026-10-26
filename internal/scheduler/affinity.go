package scheduler

import "github.com/mytecor/llm-gateway/internal/subsystem"

// BindResponsesAffinity records provider-owned response identifiers.
func (r *Runner) BindResponsesAffinity(logical, provider string, data []byte, eventType string) {
	ttl, ok := r.AffinityBindTTL(logical)
	if !ok || provider == "" || r.Affinity() == nil {
		return
	}
	if eventType == "" {
		for _, id := range subsystem.ResponseBodyAffinityIDs(data) {
			if id != "" {
				r.Affinity().Bind(id, provider, ttl)
			}
		}
		return
	}
	for _, id := range subsystem.StreamResponseAffinityIDs(data, eventType) {
		if id != "" {
			r.Affinity().Bind(id, provider, ttl)
		}
	}
}
