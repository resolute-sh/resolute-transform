package transform

import "time"

// Document is the standard schema for RAG pipelines.
// All source providers should transform their data to this format.
type Document struct {
	ID         string            `json:"id"`
	Content    string            `json:"content"`
	Title      string            `json:"title,omitempty"`
	Source     string            `json:"source"`
	URL        string            `json:"url,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	ChunkIndex int               `json:"chunk_index,omitempty"`
	ParentID   string            `json:"parent_id,omitempty"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// DocumentSource is implemented by types that can produce Documents.
type DocumentSource interface {
	ToDocuments() []Document
}

// DocumentWithEmbedding pairs a Document with its vector embedding.
type DocumentWithEmbedding struct {
	Document  Document  `json:"document"`
	Embedding []float32 `json:"embedding"`
}

// NewDocument creates a new Document with required fields.
func NewDocument(id, content, source string) Document {
	return Document{
		ID:        id,
		Content:   content,
		Source:    source,
		Metadata:  make(map[string]string),
		UpdatedAt: time.Now(),
	}
}

// WithTitle sets the document title.
func (d Document) WithTitle(title string) Document {
	d.Title = title
	return d
}

// WithURL sets the document URL.
func (d Document) WithURL(url string) Document {
	d.URL = url
	return d
}

// WithMetadata adds a metadata key-value pair.
func (d Document) WithMetadata(key, value string) Document {
	if d.Metadata == nil {
		d.Metadata = make(map[string]string)
	}
	d.Metadata[key] = value
	return d
}

// WithUpdatedAt sets the document update time.
func (d Document) WithUpdatedAt(t time.Time) Document {
	d.UpdatedAt = t
	return d
}

// AsChunk marks this document as a chunk of a parent document.
func (d Document) AsChunk(parentID string, index int) Document {
	d.ParentID = parentID
	d.ChunkIndex = index
	d.ID = parentID + "#" + string(rune('0'+index))
	return d
}

// IsChunk returns true if this document is a chunk of a larger document.
func (d Document) IsChunk() bool {
	return d.ParentID != ""
}

// DocumentBatch is a collection of documents for batch processing.
type DocumentBatch struct {
	Documents []Document
	Source    string
	Cursor    string
}

// ToDocuments implements DocumentSource for DocumentBatch.
func (b DocumentBatch) ToDocuments() []Document {
	return b.Documents
}

// Len returns the number of documents in the batch.
func (b DocumentBatch) Len() int {
	return len(b.Documents)
}

// Empty returns true if the batch has no documents.
func (b DocumentBatch) Empty() bool {
	return len(b.Documents) == 0
}
