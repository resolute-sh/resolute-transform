package transform

import (
	"github.com/resolute-sh/resolute/core"
	"go.temporal.io/sdk/worker"
)

const (
	ProviderName    = "resolute-transform"
	ProviderVersion = "1.0.0"
)

// Provider returns the transform provider for registration.
func Provider() core.Provider {
	return core.NewProvider(ProviderName, ProviderVersion).
		AddActivity("transform.Merge", MergeActivity).
		AddActivity("transform.MergeRefs", MergeRefsActivity).
		AddActivity("transform.Chunk", ChunkActivity)
}

// RegisterActivities registers all transform activities with a Temporal worker.
func RegisterActivities(w worker.Worker) {
	core.RegisterProviderActivities(w, Provider())
}
