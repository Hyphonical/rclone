package drime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"github.com/rclone/rclone/lib/rest"
)

// uploadFile uploads a file using multipart/form-data
func uploadFile(ctx context.Context, f *Fs, in io.Reader, name string, parentID int64, size int64) (*FileEntry, error) {
	// Create multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add parentId field
	err := writer.WriteField("parentId", fmt.Sprintf("%d", parentID))
	if err != nil {
		return nil, fmt.Errorf("failed to write parentId field: %w", err)
	}

	// Add file field
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	// Copy file content
	_, err = io.Copy(part, in)
	if err != nil {
		return nil, fmt.Errorf("failed to copy file content: %w", err)
	}

	// Close writer to set the terminating boundary
	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	// Prepare request
	opts := rest.Opts{
		Method:      "POST",
		Path:        "/uploads",
		Body:        body,
		ContentType: writer.FormDataContentType(),
	}

	var entry FileEntry
	var httpResp *http.Response

	err = f.pacer.Call(func() (bool, error) {
		httpResp, err = f.api.srv.CallJSON(ctx, &opts, nil, &entry)
		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	// Cache the new entry
	f.entryCacheMu.Lock()
	f.entryCache[entry.ID] = &entry
	f.entryCacheMu.Unlock()

	return &entry, nil
}