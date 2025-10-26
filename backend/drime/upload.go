package drime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

// UploadResponse is what the API actually returns
type UploadResponse struct {
	Status    string    `json:"status"`
	FileEntry FileEntry `json:"fileEntry"` // camelCase!
}

// uploadFile uploads a file using multipart/form-data
func uploadFile(ctx context.Context, f *Fs, in io.Reader, name string, parentID int64, size int64) (*FileEntry, error) {
	// Create multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add parentId field (camelCase for uploads!)
	if parentID > 0 {
		err := writer.WriteField("parentId", fmt.Sprintf("%d", parentID))
		if err != nil {
			return nil, fmt.Errorf("failed to write parentId field: %w", err)
		}
		fs.Debugf(f, "Upload: parentId=%d", parentID)
	}

	// Add file field
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	// Copy file content
	written, err := io.Copy(part, in)
	if err != nil {
		return nil, fmt.Errorf("failed to copy file content: %w", err)
	}
	fs.Debugf(f, "Upload: wrote %d bytes to multipart", written)

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

	var resp UploadResponse
	var httpResp *http.Response

	err = f.pacer.Call(func() (bool, error) {
		httpResp, err = f.api.srv.CallJSON(ctx, &opts, nil, &resp)

		if httpResp != nil {
			fs.Debugf(f, "Upload response - Status: %d, URL: %s", httpResp.StatusCode, httpResp.Request.URL)
		}
		if err != nil {
			fs.Debugf(f, "Upload error: %v", err)
		}

		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	entry := resp.FileEntry
	fs.Debugf(f, "Upload successful: ID=%d, Name=%s, Size=%d, ParentID=%v", entry.ID, entry.Name, entry.FileSize, entry.ParentID)

	// Cache the new entry
	f.entryCacheMu.Lock()
	f.entryCache[entry.ID] = &entry
	f.entryCacheMu.Unlock()

	return &entry, nil
}