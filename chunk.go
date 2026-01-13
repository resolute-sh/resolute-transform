package transform

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/resolute-sh/resolute/core"
)

// ChunkOptions configures document chunking behavior.
type ChunkOptions struct {
	// MaxTokens is the maximum number of tokens per chunk.
	// Tokens are approximated as words (space-separated).
	// Default: 512
	MaxTokens int

	// Overlap is the number of tokens to overlap between chunks.
	// Helps maintain context across chunk boundaries.
	// Default: 50
	Overlap int

	// Separator is the preferred split point within text.
	// Chunking will prefer to split at these boundaries.
	// Default: "\n\n"
	Separator string
}

// DefaultChunkOptions returns sensible defaults for chunking.
func DefaultChunkOptions() ChunkOptions {
	return ChunkOptions{
		MaxTokens: 512,
		Overlap:   50,
		Separator: "\n\n",
	}
}

// ChunkInput is the input for the Chunk transformer.
type ChunkInput struct {
	Documents []Document
	Options   ChunkOptions
}

// ChunkOutput is the output of the Chunk transformer.
type ChunkOutput struct {
	Documents []Document
	Count     int
}

// ToDocuments implements DocumentSource for ChunkOutput.
func (o ChunkOutput) ToDocuments() []Document {
	return o.Documents
}

// ChunkActivity splits large documents into smaller chunks.
func ChunkActivity(ctx context.Context, input ChunkInput) (ChunkOutput, error) {
	opts := input.Options
	if opts.MaxTokens == 0 {
		opts = DefaultChunkOptions()
	}

	var chunked []Document

	for _, doc := range input.Documents {
		chunks := chunkDocument(doc, opts)
		chunked = append(chunked, chunks...)
	}

	return ChunkOutput{
		Documents: chunked,
		Count:     len(chunked),
	}, nil
}

// Chunk creates a node that splits documents into smaller chunks.
//
// Example:
//
//	flow := core.NewFlow("pipeline").
//	    Then(fetchNode).
//	    Then(transform.Chunk(transform.ChunkOptions{MaxTokens: 512})).
//	    Then(embedNode).
//	    Build()
func Chunk(opts ChunkOptions) *core.Node[ChunkInput, ChunkOutput] {
	return core.NewNode("transform.Chunk", ChunkActivity, ChunkInput{Options: opts})
}

// MergeAndChunkInput is the input for combined merge and chunk operation.
type MergeAndChunkInput struct {
	Sources []DocumentSource
	Options ChunkOptions
}

// MergeAndChunkOutput is the output of the combined operation.
type MergeAndChunkOutput struct {
	Documents []Document
	Count     int
}

// ToDocuments implements DocumentSource for MergeAndChunkOutput.
func (o MergeAndChunkOutput) ToDocuments() []Document {
	return o.Documents
}

// MergeAndChunkActivity combines multiple sources and chunks the result.
func MergeAndChunkActivity(ctx context.Context, input MergeAndChunkInput) (MergeAndChunkOutput, error) {
	var docs []Document
	for _, source := range input.Sources {
		docs = append(docs, source.ToDocuments()...)
	}

	opts := input.Options
	if opts.MaxTokens == 0 {
		opts = DefaultChunkOptions()
	}

	var chunked []Document
	for _, doc := range docs {
		chunks := chunkDocument(doc, opts)
		chunked = append(chunked, chunks...)
	}

	return MergeAndChunkOutput{
		Documents: chunked,
		Count:     len(chunked),
	}, nil
}

// MergeAndChunk creates a node that merges sources and chunks in one step.
// This is the most common transformer for RAG pipelines.
//
// Example:
//
//	flow := core.NewFlow("knowledge-base").
//	    ThenParallel("fetch", jiraNode, confluenceNode, pagerdutyNode).
//	    Then(transform.MergeAndChunk(transform.ChunkOptions{MaxTokens: 512})).
//	    Then(embedNode).
//	    Build()
func MergeAndChunk(opts ChunkOptions) *core.Node[MergeAndChunkInput, MergeAndChunkOutput] {
	return core.NewNode("transform.MergeAndChunk", MergeAndChunkActivity, MergeAndChunkInput{Options: opts})
}

// chunkDocument splits a single document into chunks.
func chunkDocument(doc Document, opts ChunkOptions) []Document {
	content := doc.Content
	if content == "" {
		return []Document{doc}
	}

	tokens := tokenize(content, opts.Separator)
	if len(tokens) <= opts.MaxTokens {
		return []Document{doc}
	}

	var chunks []Document
	chunkIdx := 0

	for start := 0; start < len(tokens); {
		end := start + opts.MaxTokens
		if end > len(tokens) {
			end = len(tokens)
		}

		chunkContent := strings.Join(tokens[start:end], " ")

		chunk := Document{
			ID:         doc.ID + "#" + itoa(chunkIdx),
			Content:    chunkContent,
			Title:      doc.Title,
			Source:     doc.Source,
			URL:        doc.URL,
			Metadata:   copyMetadata(doc.Metadata),
			ChunkIndex: chunkIdx,
			ParentID:   doc.ID,
			UpdatedAt:  doc.UpdatedAt,
		}

		chunks = append(chunks, chunk)
		chunkIdx++

		if end >= len(tokens) {
			break
		}

		step := opts.MaxTokens - opts.Overlap
		if step < 1 {
			step = 1
		}
		start += step
	}

	return chunks
}

// tokenize splits text into tokens (words).
func tokenize(text, separator string) []string {
	paragraphs := strings.Split(text, separator)

	var tokens []string
	for _, para := range paragraphs {
		words := strings.Fields(para)
		tokens = append(tokens, words...)
	}

	return tokens
}

// copyMetadata creates a copy of metadata map.
func copyMetadata(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// itoa converts an int to string without importing strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}

	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}

	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}

	if neg {
		pos--
		buf[pos] = '-'
	}

	return string(buf[pos:])
}

// EstimateTokens estimates the number of tokens in a string.
// Uses a simple heuristic: ~4 characters per token (average for English).
func EstimateTokens(s string) int {
	return utf8.RuneCountInString(s) / 4
}
