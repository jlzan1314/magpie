package davsync

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	pathpkg "path"
	"strconv"
	"strings"
)

// remoteSize probes the remote file's byte length without downloading its body.
// It returns (-1, true, nil) when the file exists but the server did not report
// a reliable size, so callers never skip an upload on an unknown size.
func (d *dav) remoteSize(ctx context.Context, u string) (size int64, exists bool, err error) {
	res, err := d.send(ctx, http.MethodHead, u, nil, map[string]string{"Accept-Encoding": "identity"})
	if err != nil {
		return 0, false, err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return 0, false, nil
	case http.StatusOK:
		if res.ContentLength > 0 {
			return res.ContentLength, true, nil
		}
		// Some servers (such as Nutstore, #1259) answer HEAD with
		// Content-Length: 0 even for non-empty files; confirm via PROPFIND.
		if n, ok, err := d.propfindSize(ctx, u); err != nil || ok {
			return n, true, err
		}
		return -1, true, nil
	case http.StatusForbidden, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return d.propfindSize(ctx, u)
	default:
		return 0, false, fmt.Errorf("checking session file on WebDAV: HTTP %d", res.StatusCode)
	}
}

func (d *dav) propfindSize(ctx context.Context, u string) (size int64, exists bool, err error) {
	res, err := d.send(ctx, "PROPFIND", u, nil, map[string]string{"Depth": "0"})
	if err != nil {
		return 0, false, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return 0, false, nil
	case http.StatusForbidden, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return -1, false, nil
	case http.StatusMultiStatus:
	default:
		return 0, false, fmt.Errorf("checking session file on WebDAV: HTTP %d", res.StatusCode)
	}
	var ms struct {
		Responses []struct {
			Length string `xml:"propstat>prop>getcontentlength"`
		} `xml:"response"`
	}
	if err := xml.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&ms); err != nil {
		return -1, true, nil
	}
	for _, r := range ms.Responses {
		if n, err := strconv.ParseInt(strings.TrimSpace(r.Length), 10, 64); err == nil && n >= 0 {
			return n, true, nil
		}
	}
	return -1, len(ms.Responses) > 0, nil
}

type remoteSessionFile struct {
	size int64
	etag string
}

// listRemoteSizes lists direct child files in a WebDAV collection and returns
// their getcontentlength (-1 when omitted). It returns nil, nil when the server
// does not support PROPFIND so callers can fall back to per-file probes.
func (d *dav) listRemoteSizes(ctx context.Context, dirs ...string) (map[string]remoteSessionFile, error) {
	address := d.url(dirs...) + "/"
	res, err := d.send(ctx, "PROPFIND", address, nil, map[string]string{"Depth": "1"})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return map[string]remoteSessionFile{}, nil
	case http.StatusForbidden, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return nil, nil
	case http.StatusMultiStatus:
	default:
		return nil, fmt.Errorf("listing session files on WebDAV: HTTP %d", res.StatusCode)
	}
	var ms struct {
		Entries []struct {
			Href       string    `xml:"href"`
			Length     string    `xml:"propstat>prop>getcontentlength"`
			Tag        string    `xml:"propstat>prop>getetag"`
			Collection *struct{} `xml:"propstat>prop>resourcetype>collection"`
		} `xml:"response"`
	}
	if err := xml.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(&ms); err != nil {
		return nil, ctx.Err()
	}
	base, _ := url.Parse(address)
	basePath := pathpkg.Clean(base.Path)
	out := map[string]remoteSessionFile{}
	for _, e := range ms.Entries {
		if e.Collection != nil {
			continue
		}
		u, err := base.Parse(e.Href)
		if err != nil || u.Host != base.Host {
			continue
		}
		p := pathpkg.Clean(u.Path)
		if p == basePath || pathpkg.Dir(p) != basePath {
			continue
		}
		size := int64(-1)
		if n, err := strconv.ParseInt(strings.TrimSpace(e.Length), 10, 64); err == nil && n >= 0 {
			size = n
		}
		out[pathpkg.Base(p)] = remoteSessionFile{size, strings.TrimSpace(e.Tag)}
	}
	return out, nil
}

func (d *dav) remoteSizeMatches(ctx context.Context, u string, want int64, dirSizes map[string]remoteSessionFile, name string) (same, exists bool, err error) {
	if dirSizes != nil {
		sz, ok := dirSizes[name]
		if !ok {
			return false, false, nil
		}
		if sz.size >= 0 {
			return sz.size == want, true, nil
		}
	}
	sz, ex, err := d.remoteSize(ctx, u)
	if err != nil || !ex {
		return false, ex, err
	}
	return sz >= 0 && sz == want, true, nil
}
