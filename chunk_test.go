package transform

import (
	"testing"
)

func TestChunkDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		doc          Document
		opts         ChunkOptions
		wantChunks   int
		wantMultiple bool
	}{
		{
			name: "short document not chunked",
			doc: Document{
				ID:      "short-doc",
				Content: "This is a short document with few words.",
				Source:  "test",
			},
			opts:       ChunkOptions{MaxTokens: 100, Overlap: 10},
			wantChunks: 1,
		},
		{
			name: "long document chunked",
			doc: Document{
				ID:      "long-doc",
				Content: "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10 word11 word12 word13 word14 word15 word16 word17 word18 word19 word20 word21 word22 word23 word24 word25",
				Source:  "test",
			},
			opts:         ChunkOptions{MaxTokens: 10, Overlap: 2},
			wantMultiple: true,
		},
		{
			name: "empty document not chunked",
			doc: Document{
				ID:     "empty-doc",
				Source: "test",
			},
			opts:       ChunkOptions{MaxTokens: 10},
			wantChunks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			chunks := chunkDocument(tt.doc, tt.opts)

			if tt.wantMultiple {
				if len(chunks) <= 1 {
					t.Errorf("got %d chunks, want > 1", len(chunks))
				}
			} else if tt.wantChunks > 0 && len(chunks) != tt.wantChunks {
				t.Errorf("got %d chunks, want %d", len(chunks), tt.wantChunks)
			}

			for i, chunk := range chunks {
				if len(chunks) > 1 {
					if chunk.ParentID != tt.doc.ID {
						t.Errorf("chunk %d: ParentID = %q, want %q", i, chunk.ParentID, tt.doc.ID)
					}
					if chunk.ChunkIndex != i {
						t.Errorf("chunk %d: ChunkIndex = %d, want %d", i, chunk.ChunkIndex, i)
					}
				}
				if chunk.Source != tt.doc.Source {
					t.Errorf("chunk %d: Source = %q, want %q", i, chunk.Source, tt.doc.Source)
				}
			}
		})
	}
}

func TestMergeDocuments(t *testing.T) {
	t.Parallel()

	docs1 := []Document{
		{ID: "1", Source: "source1"},
		{ID: "2", Source: "source1"},
	}
	docs2 := []Document{
		{ID: "3", Source: "source2"},
	}
	docs3 := []Document{
		{ID: "4", Source: "source3"},
		{ID: "5", Source: "source3"},
	}

	merged := MergeDocuments(docs1, docs2, docs3)

	if len(merged) != 5 {
		t.Errorf("got %d documents, want 5", len(merged))
	}

	expectedIDs := []string{"1", "2", "3", "4", "5"}
	for i, doc := range merged {
		if doc.ID != expectedIDs[i] {
			t.Errorf("merged[%d].ID = %q, want %q", i, doc.ID, expectedIDs[i])
		}
	}
}

func TestDefaultChunkOptions(t *testing.T) {
	t.Parallel()

	opts := DefaultChunkOptions()

	if opts.MaxTokens != 512 {
		t.Errorf("MaxTokens = %d, want 512", opts.MaxTokens)
	}
	if opts.Overlap != 50 {
		t.Errorf("Overlap = %d, want 50", opts.Overlap)
	}
	if opts.Separator != "\n\n" {
		t.Errorf("Separator = %q, want %q", opts.Separator, "\n\n")
	}
}

func TestEstimateTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  int
	}{
		{
			name:  "empty string",
			input: "",
			want:  0,
		},
		{
			name:  "short text",
			input: "hello",
			want:  1,
		},
		{
			name:  "longer text",
			input: "This is a longer piece of text with multiple words.",
			want:  12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := EstimateTokens(tt.input)
			if got != tt.want {
				t.Errorf("EstimateTokens(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestCopyMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input map[string]string
	}{
		{
			name:  "nil map",
			input: nil,
		},
		{
			name:  "empty map",
			input: map[string]string{},
		},
		{
			name:  "populated map",
			input: map[string]string{"key1": "value1", "key2": "value2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := copyMetadata(tt.input)

			if tt.input == nil {
				if result != nil {
					t.Error("expected nil result for nil input")
				}
				return
			}

			if len(result) != len(tt.input) {
				t.Errorf("got %d entries, want %d", len(result), len(tt.input))
			}

			for k, v := range tt.input {
				if result[k] != v {
					t.Errorf("result[%q] = %q, want %q", k, result[k], v)
				}
			}

			if len(tt.input) > 0 {
				result["new-key"] = "new-value"
				if _, exists := tt.input["new-key"]; exists {
					t.Error("modifying copy affected original")
				}
			}
		})
	}
}
