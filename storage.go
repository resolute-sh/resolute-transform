package transform

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/resolute-sh/resolute/core"
)

// StoreDocuments stores a slice of Documents and returns a DataRef.
func StoreDocuments(ctx context.Context, docs []Document) (core.DataRef, error) {
	storage, err := core.GetStorage()
	if err != nil {
		return core.DataRef{}, fmt.Errorf("get storage: %w", err)
	}

	data, err := json.Marshal(docs)
	if err != nil {
		return core.DataRef{}, fmt.Errorf("marshal documents: %w", err)
	}

	ref, err := storage.StoreJSON(ctx, SchemaDocuments, docs)
	if err != nil {
		return core.DataRef{}, err
	}

	ref.Count = len(docs)
	return ref.WithChecksum(data), nil
}

// LoadDocuments loads Documents from a DataRef.
func LoadDocuments(ctx context.Context, ref core.DataRef) ([]Document, error) {
	if ref.Schema != SchemaDocuments {
		return nil, fmt.Errorf("schema mismatch: expected %s, got %s", SchemaDocuments, ref.Schema)
	}

	storage, err := core.GetStorage()
	if err != nil {
		return nil, fmt.Errorf("get storage: %w", err)
	}

	var docs []Document
	if err := storage.LoadJSON(ctx, ref, &docs); err != nil {
		return nil, fmt.Errorf("load documents: %w", err)
	}

	return docs, nil
}
