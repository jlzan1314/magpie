package davsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/fs"
	"path/filepath"

	"github.com/yetone/magpie/internal/backup"
)

// The envelope base64-encodes the gzip bytes. Leave room for that expansion
// under the per-object transport limit.
const batchPartSize = 32 << 20

type batchObject struct {
	kind string
	id   string
}

// batchParts writes one gzip stream as individually encrypted objects.
// Temporary files contain only sealed data.
type batchParts struct {
	ctx       context.Context
	dir, pass string
	size      int
	data      bytes.Buffer
	ids       []string
}

func (b *batchParts) Write(p []byte) (int, error) {
	total := 0
	for len(p) != 0 {
		n := min(len(p), b.size-b.data.Len())
		b.data.Write(p[:n])
		total += n
		p = p[n:]
		if b.data.Len() == b.size {
			if err := b.flush(); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

func (b *batchParts) flush() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if b.data.Len() == 0 {
		return nil
	}
	digest := batchDigest(b.data.Bytes())
	raw, err := sealBatchObject(b.data.Bytes(), b.pass)
	if err != nil {
		return err
	}
	if err := writeBatchFile(filepath.Join(b.dir, digest+".sealed"), raw); err != nil {
		return err
	}
	b.ids = append(b.ids, digest)
	b.data.Reset()
	return nil
}

func prepareBatchParts(ctx context.Context, dir, pass string, size int, plain []byte) ([]string, error) {
	b := &batchParts{ctx: ctx, dir: dir, pass: pass, size: size}
	z := gzip.NewWriter(b)
	var writeErr error
	for len(plain) != 0 {
		if writeErr = ctx.Err(); writeErr != nil {
			break
		}
		n := min(len(plain), 1<<20)
		if _, writeErr = z.Write(plain[:n]); writeErr != nil {
			break
		}
		plain = plain[n:]
	}
	if err := errors.Join(writeErr, z.Close()); err != nil {
		return nil, err
	}
	if err := b.flush(); err != nil {
		return nil, err
	}
	return b.ids, nil
}

func (s batchStore) keepPrepared(dir, part string, object batchObject) error {
	target := s.filename(object.kind, object.id)
	if raw, err := readBatchFile(target); err == nil {
		return s.verifyObject(raw, object.kind, object.id)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	raw, err := readBatchFile(filepath.Join(dir, part+".sealed"))
	if err != nil {
		return err
	}
	return writeBatchFile(target, raw)
}

func (s batchStore) verifyObject(raw []byte, kind, id string) error {
	if kind != "parts" {
		return s.open(raw, id, nil)
	}
	_, err := s.openPart(raw, id)
	return err
}

func (s batchStore) openPart(raw []byte, id string) ([]byte, error) {
	data, err := backup.OpenData(raw, s.passphrase)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || batchDigest(data) != id {
		return nil, errors.New("session backup part did not match its checksum")
	}
	return data, nil
}

// batchPartReader joins the gzip stream while holding only one opened part.
type batchPartReader struct {
	ctx   context.Context
	store batchStore
	ids   []string
	data  []byte
}

func (r *batchPartReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.data) == 0 {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if len(r.ids) == 0 {
			return 0, io.EOF
		}
		id := r.ids[0]
		raw, fetched, err := r.store.loadRaw(r.ctx, "parts", id)
		if err != nil {
			return 0, err
		}
		data, err := r.store.openPart(raw, id)
		if err != nil {
			return 0, err
		}
		if fetched {
			if err := writeBatchFile(r.store.filename("parts", id), raw); err != nil {
				return 0, err
			}
		}
		r.data, r.ids = data, r.ids[1:]
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}
