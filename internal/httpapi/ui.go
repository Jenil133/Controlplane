package httpapi

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"net/http"
	"path"
	"time"
)

// uiFS holds the admin UI: plain HTML, CSS and JavaScript with no build
// step, compiled into the binary so the UI always matches the API.
//
//go:embed ui
var uiFS embed.FS

// uiContentTypes are the types of the files the UI is made of, fixed here
// rather than taken from the host's MIME tables: with nosniff, a browser
// refuses a script or stylesheet served with the wrong type.
var uiContentTypes = map[string]string{
	".css":  "text/css; charset=utf-8",
	".html": "text/html; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
	".svg":  "image/svg+xml",
}

type uiFile struct {
	content     []byte
	contentType string
	etag        string
}

// uiHandler serves the embedded UI files under /ui/.
type uiHandler struct {
	files map[string]uiFile // keyed by URL path
}

func newUI() *uiHandler {
	u := &uiHandler{files: make(map[string]uiFile)}
	// The directory is embedded at build time, so walking it cannot fail.
	_ = fs.WalkDir(uiFS, "ui", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		content, err := uiFS.ReadFile(name)
		if err != nil {
			return err
		}
		contentType, ok := uiContentTypes[path.Ext(name)]
		if !ok {
			contentType = "application/octet-stream"
		}
		sum := sha256.Sum256(content)
		u.files["/"+name] = uiFile{
			content:     content,
			contentType: contentType,
			etag:        `"` + hex.EncodeToString(sum[:16]) + `"`,
		}
		return nil
	})
	return u
}

func (u *uiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if p == "/ui/" {
		p = "/ui/index.html"
	}
	f, ok := u.files[p]
	if !ok {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", f.contentType)
	h.Set("ETag", f.etag)
	// Revalidate on every load so that the UI of an upgraded binary is used
	// at once; the ETag turns an unchanged file into a 304.
	h.Set("Cache-Control", "no-cache")
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(f.content))
}
