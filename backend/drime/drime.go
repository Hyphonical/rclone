// Package drime provides an interface to Drime cloud storage
package drime

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
)

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "drime",
		Description: "Drime cloud storage",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "token",
			Help: `Access token from Drime account settings.
Leave blank to use email/password authentication.`,
			Sensitive: true,
		}, {
			Name: "email",
			Help: `Drime account email.
Only required if token is not provided.`,
		}, {
			Name:       "password",
			Help:       `Drime account password.
Only required if token is not provided.`,
			IsPassword: true,
		}},
	})
}

// Fs represents a remote drime server
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	client   *http.Client
	api      *apiClient
	pacer    *fs.Pacer

	// Path cache: maps path to entry ID
	pathCache   map[string]int64
	pathCacheMu sync.RWMutex

	// Entry cache: maps ID to FileEntry
	entryCache   map[int64]*FileEntry
	entryCacheMu sync.RWMutex

	// Root ID (null for actual root)
	rootID *int64
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Drime root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision returns the precision - 1 second as modtime is in seconds
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets - none for Drime
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	root = strings.Trim(root, "/")

	f := &Fs{
		name:        name,
		root:        root,
		opt:         *opt,
		client:      &http.Client{},
		pathCache:   make(map[string]int64),
		entryCache:  make(map[int64]*FileEntry),
		pacer:       fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		DuplicateFiles:          false,
		ReadMimeType:            false,
		WriteMimeType:           false,
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// Create API client
	f.api = newAPIClient(ctx, f)

	// Get or create access token
	var token string
	if opt.Token != "" {
		// Use provided token
		token = opt.Token
	} else {
		// Decode password if obscured
		password := opt.Password
		if password != "" {
			password, err = obscure.Reveal(password)
			if err != nil {
				return nil, fmt.Errorf("couldn't decode password: %w", err)
			}
		}

		// Authenticate with email/password
		if opt.Email == "" || password == "" {
			return nil, fmt.Errorf("either token or email+password must be provided")
		}

		token, err = f.api.login(ctx, opt.Email, password)
		if err != nil {
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	// Set bearer token for future requests
	f.api.srv.SetHeader("Authorization", "Bearer "+token)

	// Check if root exists and is a file
	if f.root != "" {
		entry, err := f.findEntry(ctx, f.root)
		if err == nil && entry.Type != "folder" {
			// Root is a file
			newRoot := path.Dir(f.root)
			if newRoot == "." {
				newRoot = ""
			}
			f.root = newRoot
			return f, fs.ErrorIsFile
		}
	}

	return f, nil
}

// findEntry finds an entry by path, using cache when possible
func (f *Fs) findEntry(ctx context.Context, remotePath string) (*FileEntry, error) {
	// Check cache first
	f.pathCacheMu.RLock()
	if id, ok := f.pathCache[remotePath]; ok {
		f.pathCacheMu.RUnlock()

		f.entryCacheMu.RLock()
		if entry, ok := f.entryCache[id]; ok {
			f.entryCacheMu.RUnlock()
			return entry, nil
		}
		f.entryCacheMu.RUnlock()
	} else {
		f.pathCacheMu.RUnlock()
	}

	// Walk the path
	parts := strings.Split(strings.Trim(remotePath, "/"), "/")
	var currentID *int64 = f.rootID

	for _, part := range parts {
		if part == "" {
			continue
		}

		// List current directory
		entries, err := f.api.listEntries(ctx, currentID)
		if err != nil {
			return nil, err
		}

		// Find matching entry
		found := false
		for i := range entries {
			if entries[i].Name == part {
				currentID = &entries[i].ID

				// Cache this entry
				f.entryCacheMu.Lock()
				f.entryCache[entries[i].ID] = &entries[i]
				f.entryCacheMu.Unlock()

				found = true
				break
			}
		}

		if !found {
			return nil, fs.ErrorObjectNotFound
		}
	}

	if currentID == nil {
		return nil, fs.ErrorObjectNotFound
	}

	// Get the final entry
	f.entryCacheMu.RLock()
	entry, ok := f.entryCache[*currentID]
	f.entryCacheMu.RUnlock()

	if !ok {
		return nil, fs.ErrorObjectNotFound
	}

	// Cache the path
	f.pathCacheMu.Lock()
	f.pathCache[remotePath] = entry.ID
	f.pathCacheMu.Unlock()

	return entry, nil
}

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirPath := path.Join(f.root, dir)

	var parentID *int64
	if dirPath != "" {
		entry, err := f.findEntry(ctx, dirPath)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				return nil, fs.ErrorDirNotFound
			}
			return nil, err
		}
		if entry.Type != "folder" {
			return nil, fs.ErrorIsFile
		}
		parentID = &entry.ID
	} else {
		parentID = f.rootID
	}

	fileEntries, err := f.api.listEntries(ctx, parentID)
	if err != nil {
		return nil, err
	}

	for i := range fileEntries {
		entry := &fileEntries[i]
		remote := path.Join(dir, entry.Name)

		// Cache entry
		f.entryCacheMu.Lock()
		f.entryCache[entry.ID] = entry
		f.entryCacheMu.Unlock()

		fullPath := path.Join(f.root, remote)
		f.pathCacheMu.Lock()
		f.pathCache[fullPath] = entry.ID
		f.pathCacheMu.Unlock()

		if entry.Type == "folder" {
			d := fs.NewDir(remote, entry.UpdatedAt).SetID(fmt.Sprintf("%d", entry.ID))
			entries = append(entries, d)
		} else {
			o := &Object{
				fs:      f,
				remote:  remote,
				id:      entry.ID,
				size:    entry.FileSize,
				modTime: entry.UpdatedAt,
			}
			entries = append(entries, o)
		}
	}

	return entries, nil
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// newObjectWithInfo creates an Object with optional metadata
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, entry *FileEntry) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}

	if entry != nil {
		return o, o.setMetadata(entry)
	}

	return o, o.readMetadata(ctx)
}

// Put uploads a new object
func (f *Fs) Put(ctx context.Context, in fs.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir makes a directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	dirPath := path.Join(f.root, dir)

	// Check if already exists
	_, err := f.findEntry(ctx, dirPath)
	if err == nil {
		return nil // Already exists
	}

	// Find parent
	parentPath := path.Dir(dirPath)
	var parentID int64 = 0

	if parentPath != "" && parentPath != "." {
		parentEntry, err := f.findEntry(ctx, parentPath)
		if err != nil {
			return err
		}
		parentID = parentEntry.ID
	}

	// Create folder
	name := path.Base(dirPath)
	entry, err := f.api.createFolder(ctx, name, parentID)
	if err != nil {
		return err
	}

	// Cache new folder
	f.entryCacheMu.Lock()
	f.entryCache[entry.ID] = entry
	f.entryCacheMu.Unlock()

	f.pathCacheMu.Lock()
	f.pathCache[dirPath] = entry.ID
	f.pathCacheMu.Unlock()

	return nil
}

// Rmdir removes a directory
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirPath := path.Join(f.root, dir)

	entry, err := f.findEntry(ctx, dirPath)
	if err != nil {
		return err
	}

	if entry.Type != "folder" {
		return fs.ErrorIsFile
	}

	// Check if empty
	entries, err := f.api.listEntries(ctx, &entry.ID)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	// Delete
	err = f.api.deleteEntries(ctx, []int64{entry.ID}, true)
	if err != nil {
		return err
	}

	// Clear cache
	f.pathCacheMu.Lock()
	delete(f.pathCache, dirPath)
	f.pathCacheMu.Unlock()

	f.entryCacheMu.Lock()
	delete(f.entryCache, entry.ID)
	f.entryCacheMu.Unlock()

	return nil
}

// Purge deletes all files in a directory
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirPath := path.Join(f.root, dir)

	entry, err := f.findEntry(ctx, dirPath)
	if err != nil {
		return err
	}

	if entry.Type != "folder" {
		return fs.ErrorIsFile
	}

	// Delete permanently
	return f.api.deleteEntries(ctx, []int64{entry.ID}, true)
}

// Copy src to this remote
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantCopy
}

// Move src to this remote
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}

	// Find destination parent
	dstPath := path.Join(f.root, remote)
	dstParentPath := path.Dir(dstPath)

	var dstParentID int64 = 0
	if dstParentPath != "" && dstParentPath != "." {
		parentEntry, err := f.findEntry(ctx, dstParentPath)
		if err != nil {
			return nil, err
		}
		dstParentID = parentEntry.ID
	}

	// Move to new parent
	err := f.api.moveEntries(ctx, []int64{srcObj.id}, dstParentID)
	if err != nil {
		return nil, err
	}

	// Rename if needed
	dstName := path.Base(dstPath)
	if dstName != srcObj.remote {
		_, err = f.api.renameEntry(ctx, srcObj.id, dstName)
		if err != nil {
			return nil, err
		}
	}

	// Create new object
	return f.NewObject(ctx, remote)
}

// DirMove moves src directory to this remote
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		return fs.ErrorCantDirMove
	}

	srcPath := path.Join(srcFs.root, srcRemote)
	dstPath := path.Join(f.root, dstRemote)

	// Find source
	srcEntry, err := srcFs.findEntry(ctx, srcPath)
	if err != nil {
		return err
	}

	// Find destination parent
	dstParentPath := path.Dir(dstPath)
	var dstParentID int64 = 0
	if dstParentPath != "" && dstParentPath != "." {
		parentEntry, err := f.findEntry(ctx, dstParentPath)
		if err != nil {
			return err
		}
		dstParentID = parentEntry.ID
	}

	// Move
	err = f.api.moveEntries(ctx, []int64{srcEntry.ID}, dstParentID)
	if err != nil {
		return err
	}

	// Rename if needed
	dstName := path.Base(dstPath)
	if dstName != srcEntry.Name {
		_, err = f.api.renameEntry(ctx, srcEntry.ID, dstName)
		if err != nil {
			return err
		}
	}

	return nil
}

// PutStream uploads with indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = (*Fs)(nil)
	_ fs.Purger      = (*Fs)(nil)
	_ fs.PutStreamer = (*Fs)(nil)
	_ fs.Mover       = (*Fs)(nil)
	_ fs.DirMover    = (*Fs)(nil)
)