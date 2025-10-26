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
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	// Drime doesn't support setting modtime directly
	// This will cause rclone to re-upload the file
	return fs.ErrorCantSetModTimeWithoutDelete
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// ID returns the ID of the object
func (o *Object) ID() string {
	return fmt.Sprintf("%d", o.id)
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
	entry, err := o.fs.findEntry(ctx, remotePath)
	if err != nil {
		return err
	}

	if entry.Type == "folder" {
		return fs.ErrorIsDir
	}

	return o.setMetadata(entry)
}

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// Get fresh metadata to ensure we have the latest URL
	err := o.readMetadata(ctx)
	if err != nil {
		return nil, err
	}

	// Get entry from cache
	o.fs.entryCacheMu.RLock()
	entry, ok := o.fs.entryCache[o.id]
	o.fs.entryCacheMu.RUnlock()

	if !ok {
		return nil, fs.ErrorObjectNotFound
	}

	return o.fs.api.download(ctx, entry, options)
}

// Update updates the object with new content
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	remotePath := path.Join(o.fs.root, o.remote)

	// Find parent directory
	parentPath := path.Dir(remotePath)
	var parentID int64 = 0

	if parentPath != "" && parentPath != "." && parentPath != "/" {
		parentEntry, err := o.fs.findEntry(ctx, parentPath)
		if err != nil {
			// Try to create parent directory
			err = o.fs.Mkdir(ctx, path.Dir(o.remote))
			if err != nil {
				return err
			}
			parentEntry, err = o.fs.findEntry(ctx, parentPath)
			if err != nil {
				return err
			}
		}
		parentID = parentEntry.ID
	}

	// Upload the file
	entry, err := uploadFile(ctx, o.fs, in, path.Base(remotePath), parentID, src.Size())
	if err != nil {
		return err
	}

	// Update object metadata
	return o.setMetadata(entry)
}

// Remove removes the object
func (o *Object) Remove(ctx context.Context) error {
	err := o.fs.api.deleteEntries(ctx, []int64{o.id}, true)
	if err != nil {
		return err
	}

	// Clear from cache
	remotePath := path.Join(o.fs.root, o.remote)
	o.fs.pathCacheMu.Lock()
	delete(o.fs.pathCache, remotePath)
	o.fs.pathCacheMu.Unlock()

	o.fs.entryCacheMu.Lock()
	delete(o.fs.entryCache, o.id)
	o.fs.entryCacheMu.Unlock()

	return nil
}

// Check the interfaces are satisfied
var _ fs.Object = (*Object)(nil)
var _ fs.IDer = (*Object)(nil)