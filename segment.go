// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// recordFile contains validated projections as JSON values separated by newlines.
// It is private scratch data, never a Store object. One batch is decoded at a time.
type recordFile struct{ *tempFile }

func (f *recordFile) each(ctx context.Context, yield func(projection) error) error {
	dec := jsontext.NewDecoder(contextReader{ctx, io.NewSectionReader(f, 0, f.Size())}, json.DefaultOptionsV1())
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var p projection
		if err := jsonv2.UnmarshalDecode(dec, &p, json.DefaultOptionsV1()); err != nil {
			if err == io.EOF {
				return nil
			}
			return &eventCodecError{"read spooled", err}
		}
		if err := yield(p); err != nil {
			return err
		}
	}
}

func (lg *Log[T]) readRecords(ctx context.Context, ref objectRef, base *Snapshot[T], yield func(projection) error) error {
	if ref.Level == 0 {
		c, err := lg.loadCommitRef(ctx, ref, base)
		if err != nil {
			return err
		}
		return yield(c.project())
	}
	f, err := lg.loadRecordFile(ctx, ref, base)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.each(ctx, yield)
}

// download spools one response without retaining it in memory. The Store reader
// is always closed, including on cancellation and local disk failures.
func (lg *Log[T]) download(ctx context.Context, ref objectRef) (_ *tempFile, err error) {
	body, err := lg.openObject(ctx, ref.Key, true)
	if err != nil {
		return nil, err
	}
	var f *tempFile
	defer func() {
		err = errors.Join(err, body.Close())
		if err != nil && f != nil {
			f.Close()
		}
	}()
	f, err = newTempFile()
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(f, contextReader{ctx, body}); err != nil {
		return nil, fmt.Errorf("download %s: %w", ref.Key, err)
	}
	return f, ctx.Err()
}

func (lg *Log[T]) loadRecordFile(ctx context.Context, ref objectRef, base *Snapshot[T]) (_ *recordFile, err error) {
	if _, _, err := lg.validateRef(ref); err != nil {
		return nil, err
	}
	f, err := newTempFile()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	enc := jsontext.NewEncoder(contextWriter{ctx, f}, json.DefaultOptionsV1())
	emit := func(p projection) error {
		if err := jsonv2.MarshalEncode(enc, &p, json.DefaultOptionsV1()); err != nil {
			return &eventCodecError{"spool", err}
		}
		return nil
	}
	if ref.Level == 0 {
		c, err := lg.loadCommitRef(ctx, ref, base)
		if err != nil {
			return nil, err
		}
		if err := emit(c.project()); err != nil {
			return nil, err
		}
	} else {
		body, err := lg.download(ctx, ref)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		if err := lg.decodeSegment(ctx, ref, body, emit); err != nil {
			return nil, err
		}
	}
	return &recordFile{f}, ctx.Err()
}

// segmentStream supplies the array incrementally. MarshalWrite omits the final
// newline, preserving the canonical v3 JSON and content-addressed keys.
type segmentStream struct {
	ref     objectRef
	records func(func(projection) error) error
}

func (s segmentStream) MarshalJSONTo(enc *jsontext.Encoder) error {
	for _, token := range []jsontext.Token{
		jsontext.BeginObject,
		jsontext.String("format"), jsontext.String(segmentFormat),
		jsontext.String("level"), jsontext.Uint(uint64(s.ref.Level)),
		jsontext.String("start"), jsontext.String(s.ref.Start),
		jsontext.String("end"), jsontext.String(s.ref.End),
		jsontext.String("records"), jsontext.BeginArray,
	} {
		if err := enc.WriteToken(token); err != nil {
			return err
		}
	}
	if err := s.records(func(p projection) error {
		return jsonv2.MarshalEncode(enc, &p, json.DefaultOptionsV1())
	}); err != nil {
		return err
	}
	if err := enc.WriteToken(jsontext.EndArray); err != nil {
		return err
	}
	return enc.WriteToken(jsontext.EndObject)
}

func (lg *Log[T]) createSegment(ctx context.Context, children []objectRef, base *Snapshot[T]) (objectRef, error) {
	if len(children) != 16 {
		return objectRef{}, corrupt("invalid compaction child count")
	}
	ref := aggregateRef(children[0].Level+1, children)
	var parts [16]*recordFile
	defer func() {
		for _, part := range parts {
			if part != nil {
				part.Close()
			}
		}
	}()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	// Four workers bound codec buffers and batch memory, regardless of level.
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			for i := worker; i < 16; i += 4 {
				if workCtx.Err() != nil {
					return
				}
				part, err := lg.loadRecordFile(workCtx, children[i], base)
				if err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
				parts[i] = part
			}
		})
	}
	wg.Wait()
	if firstErr != nil {
		return objectRef{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return objectRef{}, err
	}
	body, err := newTempFile()
	if err != nil {
		return objectRef{}, err
	}
	defer body.Close()
	enc, err := zstd.NewWriter(contextWriter{ctx, body}, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
	if err != nil {
		return objectRef{}, err
	}
	hash := sha256.New()
	next, err := parseRevision(ref.Start)
	if err != nil {
		enc.Close()
		return objectRef{}, err
	}
	chain := ref.FirstPrevHash
	stream := segmentStream{ref, func(yield func(projection) error) error {
		for i, part := range parts {
			if err := part.each(ctx, func(p projection) error {
				if p.Revision != hexRevision(next) || p.PreviousRecordHash != chain {
					return corrupt("compaction adjacency mismatch")
				}
				chain = p.RecordHash
				next++
				return yield(p)
			}); err != nil {
				return err
			}
			if err := part.Close(); err != nil {
				parts[i] = nil
				return err
			}
			parts[i] = nil
		}
		end, err := parseRevision(ref.End)
		if err != nil || next != end+1 || chain != ref.LastRecordHash {
			return corrupt("compaction boundary mismatch")
		}
		return nil
	}}
	marshalErr := jsonv2.MarshalWrite(io.MultiWriter(hash, enc), stream)
	closeErr := enc.Close()
	if err := errors.Join(marshalErr, closeErr, ctx.Err()); err != nil {
		return objectRef{}, &eventCodecError{"encode segment", err}
	}
	ref.SHA256 = hex.EncodeToString(hash.Sum(nil))
	ref.Key = lg.treeKey(ref)
	if _, _, err := lg.validateRef(ref); err != nil {
		return objectRef{}, err
	}
	if err := lg.createAggregate(ctx, ref, body); err != nil {
		return objectRef{}, err
	}
	return ref, nil
}

func (lg *Log[T]) createAggregate(ctx context.Context, ref objectRef, body SizeReaderAt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	createErr := lg.store.Create(ctx, ref.Key, body)
	if createErr == nil {
		return nil
	}
	actual, err := lg.download(ctx, ref)
	if err != nil {
		if errors.Is(err, ErrNotFound) && !errors.Is(createErr, ErrExists) {
			return fmt.Errorf("create %s: %w", ref.Key, createErr)
		}
		return err
	}
	defer actual.Close()
	return lg.decodeSegment(ctx, ref, actual, nil)
}

// decodeSegment validates a whole frame and its records while using at most
// one projection at a time. yield is internal: its output must stay unpublished
// until the final digest and envelope checks have passed.
func (lg *Log[T]) decodeSegment(ctx context.Context, ref objectRef, body SizeReaderAt, yield func(projection) error) error {
	if _, _, err := lg.validateRef(ref); err != nil {
		return err
	}
	dec, err := lg.decompress(ctx, body)
	if err != nil {
		return err
	}
	defer dec.Close()
	hash := sha256.New()
	err = lg.parseSegment(ctx, ref, io.TeeReader(dec, hash), yield)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
		return corrupt("segment digest mismatch")
	}
	return nil
}

func (lg *Log[T]) decompress(ctx context.Context, body SizeReaderAt) (*zstd.Decoder, error) {
	if _, err := singleFrame(ctx, body); err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(contextReader{ctx, io.NewSectionReader(body, 0, body.Size())}, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(1<<20), zstd.WithDecodeBuffersBelow(0))
	if err != nil {
		return nil, corrupt("invalid zstd stream")
	}
	return dec, nil
}

func (lg *Log[T]) parseSegment(ctx context.Context, ref objectRef, in io.Reader, yield func(projection) error) error {
	start, end, err := lg.validateRef(ref)
	if err != nil {
		return err
	}
	if ref.Level == 0 {
		return corrupt("expected segment reference")
	}
	// Payloads retain v1 JSON semantics, including duplicate names inside a raw
	// event. Envelope duplicates and case aliases are checked separately.
	dec := jsontext.NewDecoder(contextReader{ctx, in}, json.DefaultOptionsV1())
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return corrupt("invalid segment envelope")
	}
	seen := make(map[string]bool)
	next, chain := start, ref.FirstPrevHash
	for dec.PeekKind() != '}' {
		if err := ctx.Err(); err != nil {
			return err
		}
		tok, err := dec.ReadToken()
		if err != nil || tok.Kind() != '"' || seen[tok.String()] {
			return corrupt("invalid or duplicate segment field")
		}
		name := tok.String()
		seen[name] = true
		for _, field := range []string{"format", "level", "start", "end", "records"} {
			if name != field && strings.EqualFold(name, field) {
				return corrupt("noncanonical segment field name")
			}
		}
		switch name {
		case "format", "start", "end":
			want := map[string]string{"format": segmentFormat, "start": ref.Start, "end": ref.End}[name]
			tok, err := dec.ReadToken()
			if err != nil || tok.Kind() != '"' || tok.String() != want {
				return corrupt("segment envelope mismatch")
			}
		case "level":
			value, err := dec.ReadValue()
			var level uint8
			if err != nil || string(value) == "null" || jsonv2.Unmarshal(value, &level) != nil || level != ref.Level {
				return corrupt("segment level mismatch")
			}
		case "records":
			if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '[' {
				return corrupt("invalid segment records")
			}
			for dec.PeekKind() != ']' {
				if err := ctx.Err(); err != nil {
					return err
				}
				value, err := dec.ReadValue()
				if err != nil {
					return corrupt("invalid segment record")
				}
				var p projection
				if err := decodeJSON(value, &p); err != nil {
					return err
				}
				revision, err := lg.validateProjection(p)
				if err != nil {
					return err
				}
				if next > end || revision != next || p.PreviousRecordHash != chain {
					return corrupt("segment adjacency mismatch")
				}
				next, chain = next+1, p.RecordHash
				if yield != nil {
					if err := yield(p); err != nil {
						return err
					}
				}
			}
			if _, err := dec.ReadToken(); err != nil {
				return corrupt("invalid segment array ending")
			}
		default:
			if err := dec.SkipValue(); err != nil {
				return corrupt("invalid unknown segment field")
			}
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return corrupt("invalid segment ending")
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return corrupt("trailing or invalid segment data")
	}
	for _, field := range []string{"format", "level", "start", "end", "records"} {
		if !seen[field] {
			return corrupt("missing segment field " + field)
		}
	}
	if next != end+1 || chain != ref.LastRecordHash {
		return corrupt("segment boundary mismatch")
	}
	return nil
}

// singleFrame checks framing using fixed-size reads and offsets, rejecting
// concatenated/skippable frames and trailing data without buffering the object.
func singleFrame(ctx context.Context, body SizeReaderAt) (zstd.Header, error) {
	var header [18]byte // Maximum zstd frame header length.
	n, err := body.ReadAt(header[:], 0)
	var h zstd.Header
	if err != nil && err != io.EOF {
		return h, err
	}
	if err := h.Decode(header[:n]); err != nil || h.Skippable {
		return h, corrupt("invalid zstd frame header")
	}
	pos, size := int64(h.HeaderSize), body.Size()
	var block [3]byte
	for {
		if err := ctx.Err(); err != nil {
			return h, err
		}
		if pos > size-3 {
			return h, corrupt("truncated zstd block")
		}
		if _, err := body.ReadAt(block[:], pos); err != nil {
			return h, err
		}
		bits := uint32(block[0]) | uint32(block[1])<<8 | uint32(block[2])<<16
		pos += 3
		blockSize := int64(bits >> 3)
		switch (bits >> 1) & 3 {
		case 1:
			blockSize = 1
		case 3:
			return h, corrupt("reserved zstd block type")
		}
		if blockSize > size-pos {
			return h, corrupt("truncated zstd block data")
		}
		pos += blockSize
		if bits&1 != 0 {
			break
		}
	}
	if h.HasCheckSum {
		if size-pos < 4 {
			return h, corrupt("truncated zstd checksum")
		}
		pos += 4
	}
	if pos != size {
		return h, corrupt("trailing zstd data")
	}
	return h, nil
}
