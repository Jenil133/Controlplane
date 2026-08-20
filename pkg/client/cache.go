package client

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"google.golang.org/protobuf/encoding/protojson"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// maxCacheBytes bounds the cache file read at start: protojson is larger than
// the wire form of the snapshots the client accepts, but not by this much. A
// file beyond it is not ours, or damaged, and reading it all would only cost
// memory.
const maxCacheBytes = 2 * maxSnapshotBytes

// loadCache starts serving the snapshot in the cache file, if it holds a
// usable one.
func (c *Client) loadCache() {
	b, err := readCacheFile(c.cachePath)
	if errors.Is(err, fs.ErrNotExist) {
		c.log.Info("no control plane snapshot cache yet", "path", c.cachePath)
		return
	}
	if err != nil {
		c.log.Warn("ignoring unreadable control plane snapshot cache", "path", c.cachePath, "error", err)
		return
	}
	snap := &cpv1.Snapshot{}
	// A newer SDK may have written fields this one does not know; the rest
	// of the snapshot is still good.
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, snap); err != nil {
		c.log.Warn("ignoring corrupt control plane snapshot cache", "path", c.cachePath, "error", err)
		return
	}
	c.install(snap, SourceCache)
}

func readCacheFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxCacheBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxCacheBytes {
		return nil, errors.New("file too large")
	}
	return b, nil
}

// writeCache stores snap at path so that, whenever the process or machine
// stops, the file holds either the previous snapshot or this one in full.
func writeCache(path string, snap *cpv1.Snapshot) error {
	b, err := protojson.Marshal(snap)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// The temporary file must be in the same directory: a rename is only
	// atomic within one file system.
	tmp, err := writeTemp(dir, filepath.Base(path), b)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	// The rename itself is durable only once the directory is synced.
	return syncDir(dir)
}

// writeTemp writes b to a new file in dir, readable only by its owner, and
// flushes it to disk.
func writeTemp(dir, base string, b []byte) (name string, err error) {
	f, err := os.CreateTemp(dir, "."+base+".*.tmp") // mode 0600
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	if _, err := f.Write(b); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", err
	}
	return f.Name(), f.Close()
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		// Windows cannot flush a directory handle.
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
