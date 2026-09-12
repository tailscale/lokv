// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

func (lg *Log[T]) commitRef(s *Snapshot[T]) objectRef {
	p := s.commit.project()
	return objectRef{0, p.Revision, p.Revision, lg.logKey(s.Revision()), digest(s.body), p.PreviousRecordHash, p.RecordHash}
}

func (lg *Log[T]) carry(ctx context.Context, base *Snapshot[T]) ([]frontierLevel, error) {
	var levels [16][]objectRef
	for _, f := range base.commit.Frontier {
		levels[f.Level] = append([]objectRef(nil), f.Refs...)
	}
	carry := lg.commitRef(base)
	for level := 0; level < 16; level++ {
		levels[level] = append(levels[level], carry)
		if len(levels[level]) < 16 {
			break
		}
		var err error
		carry, err = lg.createSegment(ctx, levels[level], base)
		if err != nil {
			return nil, err
		}
		levels[level] = nil
	}
	frontier := make([]frontierLevel, 0, 16)
	for level, refs := range levels {
		if len(refs) != 0 {
			frontier = append(frontier, frontierLevel{uint8(level), refs})
		}
	}
	return frontier, nil
}

func aggregateRef(level uint8, children []objectRef) objectRef {
	return objectRef{Level: level, Start: children[0].Start, End: children[len(children)-1].End, FirstPrevHash: children[0].FirstPrevHash, LastRecordHash: children[len(children)-1].LastRecordHash}
}

func (lg *Log[T]) loadCommitRef(ctx context.Context, ref objectRef, cached *Snapshot[T]) (*commit, error) {
	if _, _, err := lg.validateRef(ref); err != nil {
		return nil, err
	}
	if ref.Level != 0 {
		return nil, corrupt("expected commit reference")
	}
	var b []byte
	if cached != nil && ref.Key == lg.logKey(cached.Revision()) {
		b = cached.body
	} else {
		var err error
		b, err = lg.get(ctx, ref.Key, true)
		if err != nil {
			return nil, err
		}
	}
	if digest(b) != ref.SHA256 {
		return nil, corrupt("commit object digest mismatch")
	}
	c, err := lg.decodeCommit(ref.Key, b)
	if err != nil {
		return nil, err
	}
	p := c.project()
	if p.PreviousRecordHash != ref.FirstPrevHash || p.RecordHash != ref.LastRecordHash {
		return nil, corrupt("commit reference hash boundary mismatch")
	}
	return c, nil
}

// loadRecords reads a complete referenced range without consulting its older
// constituent objects. Level 0 is a commit; every higher level is a packed segment.
func (lg *Log[T]) loadRecords(ctx context.Context, ref objectRef, base *Snapshot[T]) ([]projection, error) {
	if ref.Level == 0 {
		c, err := lg.loadCommitRef(ctx, ref, base)
		if err != nil {
			return nil, err
		}
		return []projection{c.project()}, nil
	}
	if _, _, err := lg.validateRef(ref); err != nil {
		return nil, err
	}
	body, err := lg.get(ctx, ref.Key, true)
	if err != nil {
		return nil, err
	}
	seg, err := lg.decodeSegment(ref, body)
	if err != nil {
		return nil, err
	}
	return seg.Records, nil
}

func (lg *Log[T]) createSegment(ctx context.Context, children []objectRef, base *Snapshot[T]) (objectRef, error) {
	ref := aggregateRef(children[0].Level+1, children)
	var parts [16][]projection
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// At most four GETs are in flight. Each worker owns distinct output slots;
	// cancellation terminates the whole group and no goroutines outlive this call.
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			for i := worker; i < 16; i += 4 {
				if workCtx.Err() != nil {
					return
				}
				records, err := lg.loadRecords(workCtx, children[i], base)
				if err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
				parts[i] = records
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
	var records []projection
	for _, part := range parts {
		records = append(records, part...)
	}
	seg := segment{segmentFormat, ref.Level, ref.Start, ref.End, records}
	raw, err := json.Marshal(seg)
	if err != nil {
		return objectRef{}, errors.New("lokv: encode segment")
	}
	ref.SHA256 = digest(raw)
	ref.Key = lg.treeKey(ref)
	if err := lg.validateSegment(ref, &seg); err != nil {
		return objectRef{}, err
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
	if err != nil {
		return objectRef{}, err
	}
	body := enc.EncodeAll(raw, nil)
	enc.Close()
	if err := lg.createAggregate(ctx, ref, body); err != nil {
		return objectRef{}, err
	}
	return ref, nil
}

func (lg *Log[T]) createAggregate(ctx context.Context, ref objectRef, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	createErr := lg.store.Create(ctx, ref.Key, body)
	if createErr == nil {
		return nil
	}
	actual, err := lg.get(ctx, ref.Key, true)
	if err != nil {
		if errors.Is(err, ErrNotFound) && !errors.Is(createErr, ErrExists) {
			return fmt.Errorf("create %s: %w", ref.Key, createErr)
		}
		return err
	}
	_, err = lg.decodeSegment(ref, actual)
	return err
}

// singleFrame checks the zstd frame/block framing before decompression, rejecting
// concatenated or skippable frames even though a general zstd decoder allows them.
func singleFrame(b []byte) (zstd.Header, error) {
	var h zstd.Header
	if err := h.Decode(b); err != nil || h.Skippable {
		return h, corrupt("invalid zstd frame header")
	}
	pos := h.HeaderSize
	for {
		if pos > len(b)-3 {
			return h, corrupt("truncated zstd block")
		}
		bits := uint32(b[pos]) | uint32(b[pos+1])<<8 | uint32(b[pos+2])<<16
		pos += 3
		size := int(bits >> 3)
		switch (bits >> 1) & 3 {
		case 1:
			size = 1
		case 3:
			return h, corrupt("reserved zstd block type")
		}
		if size > len(b)-pos {
			return h, corrupt("truncated zstd block data")
		}
		pos += size
		if bits&1 != 0 {
			break
		}
	}
	if h.HasCheckSum {
		pos += 4
	}
	if pos != len(b) {
		return h, corrupt("trailing or truncated zstd data")
	}
	return h, nil
}

func (lg *Log[T]) decompress(body []byte) ([]byte, error) {
	if _, err := singleFrame(body); err != nil {
		return nil, err
	}
	// Bound compression history, not output size. Upper levels may contain
	// arbitrarily large ranges. The reader API avoids DecodeAll's output cap.
	dec, err := zstd.NewReader(bytes.NewReader(body), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(1<<20), zstd.WithDecodeBuffersBelow(0))
	if err != nil {
		return nil, corrupt("invalid zstd stream")
	}
	defer dec.Close()
	raw, err := io.ReadAll(dec)
	if err != nil {
		return nil, corrupt("invalid zstd data")
	}
	return raw, nil
}

func (lg *Log[T]) validateSegment(ref objectRef, seg *segment) error {
	start, end, err := lg.validateRef(ref)
	if err != nil {
		return err
	}
	if ref.Level == 0 || seg.Format != segmentFormat || seg.Level != ref.Level || seg.Start != ref.Start || seg.End != ref.End || int64(len(seg.Records)) != end-start+1 {
		return corrupt("segment envelope mismatch")
	}
	chain := ref.FirstPrevHash
	for i, p := range seg.Records {
		revision, err := lg.validateProjection(p)
		if err != nil {
			return err
		}
		if revision != start+int64(i) || p.PreviousRecordHash != chain {
			return corrupt("segment adjacency mismatch")
		}
		chain = p.RecordHash
	}
	if chain != ref.LastRecordHash {
		return corrupt("segment hash boundary mismatch")
	}
	return nil
}

func (lg *Log[T]) decodeSegment(ref objectRef, body []byte) (*segment, error) {
	raw, err := lg.decompress(body)
	if err != nil {
		return nil, err
	}
	if digest(raw) != ref.SHA256 {
		return nil, corrupt("segment digest mismatch")
	}
	var seg segment
	if err := decodeJSON(raw, &seg); err != nil {
		return nil, err
	}
	if err := lg.validateSegment(ref, &seg); err != nil {
		return nil, err
	}
	return &seg, nil
}
