package transform

import (
	"context"

	"github.com/resolute-sh/resolute/core"
)

// MergeInput is the input for the Merge transformer.
type MergeInput struct {
	Sources []DocumentSource
}

// MergeOutput is the output of the Merge transformer.
type MergeOutput struct {
	Documents []Document
	Count     int
}

// ToDocuments implements DocumentSource for MergeOutput.
func (o MergeOutput) ToDocuments() []Document {
	return o.Documents
}

// MergeActivity combines multiple DocumentSource outputs into a single document list.
func MergeActivity(ctx context.Context, input MergeInput) (MergeOutput, error) {
	var docs []Document

	for _, source := range input.Sources {
		docs = append(docs, source.ToDocuments()...)
	}

	return MergeOutput{
		Documents: docs,
		Count:     len(docs),
	}, nil
}

// Merge creates a node that combines multiple DocumentSource outputs.
// This is typically used after ThenParallel to merge results from multiple sources.
//
// Example:
//
//	flow := core.NewFlow("pipeline").
//	    ThenParallel("fetch", jiraNode, confluenceNode).
//	    Then(transform.Merge()).
//	    Then(embedNode).
//	    Build()
func Merge() *core.Node[MergeInput, MergeOutput] {
	return core.NewNode("transform.Merge", MergeActivity, MergeInput{})
}

// MergeDocuments is a utility function to merge document slices directly.
func MergeDocuments(sources ...[]Document) []Document {
	var total int
	for _, s := range sources {
		total += len(s)
	}

	docs := make([]Document, 0, total)
	for _, s := range sources {
		docs = append(docs, s...)
	}

	return docs
}

// MergeSources merges DocumentSource implementations into a single document list.
func MergeSources(sources ...DocumentSource) []Document {
	var total int
	for _, s := range sources {
		total += len(s.ToDocuments())
	}

	docs := make([]Document, 0, total)
	for _, s := range sources {
		docs = append(docs, s.ToDocuments()...)
	}

	return docs
}

// MergeRefsInput is the input for MergeRefsActivity.
type MergeRefsInput struct {
	Refs []core.DataRef
}

// MergeRefsOutput is the output of MergeRefsActivity.
type MergeRefsOutput struct {
	Ref   core.DataRef
	Count int
}

// MergeRefsActivity merges documents from multiple DataRefs into a single DataRef.
func MergeRefsActivity(ctx context.Context, input MergeRefsInput) (MergeRefsOutput, error) {
	var allDocs []Document

	for _, ref := range input.Refs {
		docs, err := LoadDocuments(ctx, ref)
		if err != nil {
			return MergeRefsOutput{}, err
		}
		allDocs = append(allDocs, docs...)
	}

	mergedRef, err := StoreDocuments(ctx, allDocs)
	if err != nil {
		return MergeRefsOutput{}, err
	}

	return MergeRefsOutput{
		Ref:   mergedRef,
		Count: len(allDocs),
	}, nil
}

// MergeRefs creates a node that merges documents from multiple DataRefs.
func MergeRefs(input MergeRefsInput) *core.Node[MergeRefsInput, MergeRefsOutput] {
	return core.NewNode("transform.MergeRefs", MergeRefsActivity, input)
}
