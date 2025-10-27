package drime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

const (
	apiBaseURL = "https://app.drime.cloud/api/v1"
)

// API response structures based on OpenAPI spec

// LoginRequest for authentication
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// LoginResponse contains the access token
type LoginResponse struct {
	Status string `json:"status"`
	User   User   `json:"user"`
}

// User represents a Drime user
type User struct {
	ID          int64  `json:"id"`
	AccessToken string `json:"access_token"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

// FileEntry represents a file or folder from the API
type FileEntry struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	FileSize  int64     `json:"file_size"`
	FileHash  *string   `json:"file_hash"` // Always null per spec
	UpdatedAt time.Time `json:"updated_at"`
	ParentID  *int64    `json:"parent_id"`
	URL       string    `json:"url"` // For downloads
}

// ListEntriesResponse for paginated listing
type ListEntriesResponse struct {
	Data        []FileEntry `json:"data"`
	CurrentPage int         `json:"current_page"`
	LastPage    int         `json:"last_page"`
}

// CreateFolderRequest for making directories
type CreateFolderRequest struct {
	Name     string `json:"name"`
	ParentID int64  `json:"parent_id"`
}

// CreateFolderResponse returned after creating folder
type CreateFolderResponse struct {
	Folder FileEntry `json:"folder"`
}

// DeleteRequest for deleting files/folders
type DeleteRequest struct {
	EntryIDs      []int64 `json:"entryIds"`
	DeleteForever bool    `json:"deleteForever"`
}

// RenameRequest for renaming items
type RenameRequest struct {
	Name string `json:"name"`
}

// MoveRequest for moving items
type MoveRequest struct {
	EntryIDs      []int64 `json:"entryIds"`
	DestinationID int64   `json:"destinationId"`
}

// apiClient wraps the REST client
type apiClient struct {
	srv *rest.Client
	f   *Fs
}

// newAPIClient creates a new API client
func newAPIClient(ctx context.Context, f *Fs) *apiClient {
	client := &apiClient{
		f: f,
	}

	// Create REST client with bearer token
	client.srv = rest.NewClient(f.client).SetRoot(apiBaseURL)

	return client
}

// login performs authentication and returns access token
func (c *apiClient) login(ctx context.Context, email, password string) (token string, err error) {
	req := LoginRequest{
		Email:    email,
		Password: password,
	}

	// Create a temporary client without auth header for login
	loginClient := rest.NewClient(c.f.client).SetRoot(apiBaseURL)

	opts := rest.Opts{
		Method: "POST",
		Path:   "/auth/login",
	}

	var resp LoginResponse
	var httpResp *http.Response

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = loginClient.CallJSON(ctx, &opts, &req, &resp)
		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return "", fmt.Errorf("login failed: %w", err)
	}

	if resp.User.AccessToken == "" {
		return "", fmt.Errorf("no access token in login response")
	}

	return resp.User.AccessToken, nil
}

// listEntries lists files and folders in a directory
func (c *apiClient) listEntries(ctx context.Context, parentID *int64) ([]FileEntry, error) {
	var allEntries []FileEntry
	page := 1

	for {
		opts := rest.Opts{
			Method: "GET",
			Path:   "/drive/file-entries",
			Parameters: url.Values{
				"page": []string{fmt.Sprintf("%d", page)},
			},
		}

		if parentID != nil {
			opts.Parameters.Set("parentId", fmt.Sprintf("%d", *parentID))
		}

		var resp ListEntriesResponse
		var httpResp *http.Response
		var err error

		err = c.f.pacer.Call(func() (bool, error) {
			httpResp, err = c.srv.CallJSON(ctx, &opts, nil, &resp)
			return shouldRetry(ctx, httpResp, err)
		})

		if err != nil {
			return nil, err
		}

		allEntries = append(allEntries, resp.Data...)

		fs.Debugf(c.f, "listEntries: page=%d, got=%d entries, total=%d, last_page=%d",
			page, len(resp.Data), len(allEntries), resp.LastPage)

		// Check if we've reached the last page
		if page >= resp.LastPage {
			break
		}

		page++
	}

	return allEntries, nil
}

// getEntry gets metadata for a single entry by ID
func (c *apiClient) getEntry(ctx context.Context, id int64) (*FileEntry, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   fmt.Sprintf("/file-entries/%d", id),
	}

	var entry FileEntry
	var httpResp *http.Response
	var err error

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = c.srv.CallJSON(ctx, &opts, nil, &entry)
		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return nil, err
	}

	return &entry, nil
}

// download downloads a file - must follow redirect
func (c *apiClient) download(ctx context.Context, entry *FileEntry, options []fs.OpenOption) (io.ReadCloser, error) {
	downloadURL := entry.URL

	// Check if URL is absolute or relative
	if !strings.HasPrefix(downloadURL, "http://") && !strings.HasPrefix(downloadURL, "https://") {
		// Relative URL - need to construct full URL
		// Remove leading slash if present
		downloadURL = strings.TrimPrefix(downloadURL, "/")

		// Construct full URL
		if strings.HasPrefix(downloadURL, "api/") {
			// URL like "api/v1/file-entries/123" -> https://app.drime.cloud/api/v1/file-entries/123
			downloadURL = "https://app.drime.cloud/" + downloadURL
		} else {
			// Other relative URLs
			downloadURL = apiBaseURL + "/" + downloadURL
		}
	}

	fs.Debugf(c.f, "Downloading from URL: %s", downloadURL)

	opts := rest.Opts{
		Method:  "GET",
		RootURL: downloadURL,
		Options: options,
	}

	var httpResp *http.Response
	var err error

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = c.srv.Call(ctx, &opts)
		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return nil, fmt.Errorf("download failed: %w", err)
	}

	return httpResp.Body, nil
}

// createFolder creates a new folder
func (c *apiClient) createFolder(ctx context.Context, name string, parentID int64) (*FileEntry, error) {
	// First check if folder already exists
	var checkParentID *int64
	if parentID == 0 {
		checkParentID = nil // Root folder
	} else {
		checkParentID = &parentID
	}

	fs.Debugf(c.f, "Checking if folder exists: name=%s, parentID=%d", name, parentID)
	entries, err := c.listEntries(ctx, checkParentID)
	if err == nil {
		fs.Debugf(c.f, "Found %d entries in parent %v", len(entries), checkParentID)
		for i := range entries {
			if entries[i].Name == name && entries[i].Type == "folder" {
				fs.Debugf(c.f, "Folder already exists: %s (ID: %d)", name, entries[i].ID)
				return &entries[i], nil
			}
		}
	}

	req := CreateFolderRequest{
		Name:     name,
		ParentID: parentID,
	}

	fs.Debugf(c.f, "Creating folder: name=%s, parentID=%d", name, parentID)

	opts := rest.Opts{
		Method: "POST",
		Path:   "/folders",
	}

	var resp CreateFolderResponse
	var httpResp *http.Response

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = c.srv.CallJSON(ctx, &opts, &req, &resp)

		if httpResp != nil {
			fs.Debugf(c.f, "Create folder response - Status: %d, URL: %s", httpResp.StatusCode, httpResp.Request.URL)
		}
		if err != nil {
			fs.Debugf(c.f, "Create folder error: %v", err)
		}

		// Don't retry 422 errors
		if httpResp != nil && httpResp.StatusCode == 422 {
			return false, err
		}

		return shouldRetry(ctx, httpResp, err)
	})

	// If we got a 422, try multiple times to find the folder (may need time to appear in listing)
	if err != nil && httpResp != nil && httpResp.StatusCode == 422 {
		fs.Debugf(c.f, "Got 422, searching for existing folder with retries: name=%s, parentID=%v", name, checkParentID)

		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Millisecond * 500) // Wait before retry
			}

			entries, listErr := c.listEntries(ctx, checkParentID)
			if listErr == nil {
				fs.Debugf(c.f, "Attempt %d: Found %d entries", attempt+1, len(entries))
				for i := range entries {
					if entries[i].Name == name && entries[i].Type == "folder" {
						fs.Debugf(c.f, "Found existing folder: %s (ID: %d)", name, entries[i].ID)
						return &entries[i], nil
					}
				}
			}
		}

		fs.Debugf(c.f, "Failed to find folder after 422 error")
		return nil, fmt.Errorf("create folder failed: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("create folder failed: %w", err)
	}

	fs.Debugf(c.f, "Folder created successfully: %+v", resp.Folder)

	return &resp.Folder, nil
}

// deleteEntries deletes files/folders
func (c *apiClient) deleteEntries(ctx context.Context, ids []int64, permanent bool) error {
	req := DeleteRequest{
		EntryIDs:      ids,
		DeleteForever: permanent,
	}

	fs.Debugf(c.f, "Deleting entries: ids=%v, permanent=%v", ids, permanent)

	// Use POST to /file-entries/delete (not DELETE /file-entries!)
	opts := rest.Opts{
		Method: "POST",
		Path:   "/file-entries/delete",
	}

	var httpResp *http.Response
	var err error

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = c.srv.CallJSON(ctx, &opts, &req, nil)

		if httpResp != nil {
			fs.Debugf(c.f, "Delete response - Status: %d, URL: %s", httpResp.StatusCode, httpResp.Request.URL)
		}
		if err != nil {
			fs.Debugf(c.f, "Delete error: %v", err)
		}

		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	fs.Debugf(c.f, "Delete successful")

	return nil
}

// renameEntry renames a file or folder
func (c *apiClient) renameEntry(ctx context.Context, id int64, newName string) (*FileEntry, error) {
	req := RenameRequest{
		Name: newName,
	}

	opts := rest.Opts{
		Method: "PUT",
		Path:   fmt.Sprintf("/file-entries/%d", id),
	}

	var entry FileEntry
	var httpResp *http.Response
	var err error

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = c.srv.CallJSON(ctx, &opts, &req, &entry)
		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return nil, fmt.Errorf("rename failed: %w", err)
	}

	return &entry, nil
}

// moveEntries moves files/folders to a new parent
func (c *apiClient) moveEntries(ctx context.Context, ids []int64, destinationID int64) error {
	req := MoveRequest{
		EntryIDs:      ids,
		DestinationID: destinationID,
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/file-entries/move",
	}

	var httpResp *http.Response
	var err error

	err = c.f.pacer.Call(func() (bool, error) {
		httpResp, err = c.srv.CallJSON(ctx, &opts, &req, nil)
		return shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return fmt.Errorf("move failed: %w", err)
	}

	return nil
}

// shouldRetry determines if an error should be retried
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if err != nil {
		// Check for context errors
		if ctx.Err() != nil {
			return false, err
		}
		// Retry on temporary errors
		return true, err
	}

	// Retry on rate limit and server errors
	if resp != nil && (resp.StatusCode == 429 || resp.StatusCode >= 500) {
		return true, err
	}

	// Check for specific HTTP status codes
	if resp != nil {
		switch resp.StatusCode {
		case 401, 403, 404, 422:
			// Don't retry auth failures, not found, or validation errors
			return false, err
		}
	}

	return false, err
}