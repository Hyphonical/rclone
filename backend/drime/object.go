package drime

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

// Object represents a Drime file
type Object struct {
	fs      *Fs
	remote  string
	id      int64
	size    int64
	modTime time.Time
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the hash of the object (not supported)
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of the object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// ID returns the ID of the object
func (o *Object) ID() string {
	return fmt.Sprintf("%d", o.id)
}

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	entry, err := o.fs.api.getEntry(ctx, o.id)
	if err != nil {
		return nil, err
	}
	return o.fs.api.download(ctx, entry, options)
}

// Update updates the object with new content
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// remotePath is the full path to the file, which is what the API's relativePath expects.
	remotePath := path.Join(o.fs.root, o.remote)
	parentPath := path.Dir(remotePath)

	var parentID int64
	if parentPath != "" && parentPath != "." {
		// Mkdir will ensure the parent directory exists, handling concurrency.
		if err := o.fs.Mkdir(ctx, path.Dir(o.remote)); err != nil {
			return fmt.Errorf("failed to make parent directory: %w", err)
		}

		parentEntry, err := o.fs.findEntry(ctx, parentPath)
		if err != nil {
			return fmt.Errorf("failed to find parent entry: %w", err)
		}
		parentID = parentEntry.ID
	}

	entry, err := o.fs.api.uploadFile(ctx, in, path.Base(o.remote), parentID, src.Size(), remotePath)
	if err != nil {
		return err
	}

	return o.setMetadata(entry)
}

// Remove removes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.api.deleteEntries(ctx, []int64{o.id}, true)
}

// setMetadata sets the metadata from an API entry
func (o *Object) setMetadata(entry *FileEntry) error {
	o.id = entry.ID
	o.size = entry.FileSize
	o.modTime = entry.UpdatedAt
	return nil
}

// readMetadata reads the metadata for this object
func (o *Object) readMetadata(ctx context.Context) error {
	remotePath := path.Join(o.fs.root, o.remote)
	dirPath := path.Dir(remotePath)
	fileName := path.Base(remotePath)

	var parentID *int64
	if dirPath != "" && dirPath != "." {
		parentEntry, err := o.fs.findEntry(ctx, dirPath)
		if err != nil {
			return err
		}
		parentID = &parentEntry.ID
	}

	entries, err := o.fs.api.listEntries(ctx, parentID)
	if err != nil {
		return err
	}

	for i := range entries {
		if entries[i].Name == fileName && entries[i].Type != "folder" {
			return o.setMetadata(&entries[i])
		}
	}

	return fs.ErrorObjectNotFound
}

// Check the interfaces are satisfied
var _ fs.Object = (*Object)(nil)
var _ fs.IDer = (*Object)(nil)