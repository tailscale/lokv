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
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

func (l *Log[T]) commitRef(s *Snapshot[T]) objectRef {
	p := s.commit.project()
	return objectRef{0, p.Revision, p.Revision, l.logKey(s.Revision()), digest(s.body), p.PreviousRecordHash, p.RecordHash}
}

func (l *Log[T]) carry(ctx context.Context, base *Snapshot[T]) ([]frontierLevel, error) {
	var levels [16][]objectRef
	for _, f := range base.commit.Frontier {
		levels[f.Level] = append([]objectRef(nil), f.Refs...)
	}
	carry := l.commitRef(base)
	for level := 0; level < 16; level++ {
		levels[level] = append(levels[level], carry)
		if len(levels[level]) < 16 {
			break
		}
		var err error
		if level == 0 {
			carry, err = l.createSegment(ctx, levels[0], base)
		} else {
			carry, err = l.createIndex(ctx, uint8(level+1), levels[level])
		}
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

func (l *Log[T]) validateChildren(ref objectRef, children []objectRef) error {
	start, end, err := l.validateRef(ref)
	if err != nil {
		return err
	}
	if ref.Level < 2 || len(children) != 16 {
		return corrupt("invalid index child count or level")
	}
	next, chain := start, ref.FirstPrevHash
	for i, child := range children {
		cs, ce, err := l.validateRef(child)
		if err != nil {
			return err
		}
		if child.Level != ref.Level-1 || cs != next || ce > end || child.FirstPrevHash != chain {
			return corrupt("index adjacency mismatch")
		}
		chain = child.LastRecordHash
		if i == 15 {
			if ce != end {
				return corrupt("index end mismatch")
			}
		} else {
			next = ce + 1
		}
	}
	if chain != ref.LastRecordHash {
		return corrupt("index hash boundary mismatch")
	}
	return nil
}

func (l *Log[T]) loadCommitRef(ctx context.Context, ref objectRef, cached *Snapshot[T]) (*commit, error) {
	if _, _, err := l.validateRef(ref); err != nil {
		return nil, err
	}
	if ref.Level != 0 {
		return nil, corrupt("expected commit reference")
	}
	var b []byte
	if cached != nil && ref.Key == l.logKey(cached.Revision()) {
		b = cached.body
	} else {
		var err error
		b, err = l.get(ctx, ref.Key, true)
		if err != nil {
			return nil, err
		}
	}
	if digest(b) != ref.SHA256 {
		return nil, corrupt("commit object digest mismatch")
	}
	c, err := l.decodeCommit(ref.Key, b)
	if err != nil {
		return nil, err
	}
	p := c.project()
	if p.PreviousRecordHash != ref.FirstPrevHash || p.RecordHash != ref.LastRecordHash {
		return nil, corrupt("commit reference hash boundary mismatch")
	}
	return c, nil
}

func (l *Log[T]) createSegment(ctx context.Context, children []objectRef, base *Snapshot[T]) (objectRef, error) {
	ref := aggregateRef(1, children)
	records := make([]projection, 16)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// At most four GETs are in flight. Each worker owns distinct output slots;
	// cancellation terminates the whole group and no goroutines outlive this call.
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	var eventBytes atomic.Int64
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			for i := worker; i < 16; i += 4 {
				if workCtx.Err() != nil {
					return
				}
				c, err := l.loadCommitRef(workCtx, children[i], base)
				if err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
				if eventBytes.Add(int64(len(c.Event))) > l.maxObject {
					once.Do(func() { firstErr = ErrTooLarge; cancel() })
					return
				}
				records[i] = c.project()
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
	seg := segment{segmentFormat, 1, ref.Start, ref.End, records}
	raw, err := json.Marshal(seg)
	if err != nil {
		return objectRef{}, errors.New("lokv: encode segment")
	}
	if int64(len(raw)) > l.maxObject {
		return objectRef{}, ErrTooLarge
	}
	ref.SHA256 = digest(raw)
	ref.Key = l.treeKey(ref)
	if err := l.validateSegment(ref, &seg); err != nil {
		return objectRef{}, err
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithWindowSize(1<<20), zstd.WithEncoderCRC(true))
	if err != nil {
		return objectRef{}, err
	}
	body := enc.EncodeAll(raw, nil)
	enc.Close()
	if err := l.createAggregate(ctx, ref, body); err != nil {
		return objectRef{}, err
	}
	return ref, nil
}

func (l *Log[T]) createIndex(ctx context.Context, level uint8, children []objectRef) (objectRef, error) {
	ref := aggregateRef(level, children)
	node := indexNode{indexFormat, level, ref.Start, ref.End, children}
	body, err := json.Marshal(node)
	if err != nil {
		return objectRef{}, errors.New("lokv: encode index")
	}
	ref.SHA256 = digest(body)
	ref.Key = l.treeKey(ref)
	if err := l.validateChildren(ref, children); err != nil {
		return objectRef{}, err
	}
	if err := l.createAggregate(ctx, ref, body); err != nil {
		return objectRef{}, err
	}
	return ref, nil
}

func (l *Log[T]) createAggregate(ctx context.Context, ref objectRef, body []byte) error {
	if int64(len(body)) > l.maxObject {
		return ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	createErr := l.store.Create(ctx, ref.Key, body)
	if createErr == nil {
		return nil
	}
	actual, err := l.get(ctx, ref.Key, true)
	if err != nil {
		if errors.Is(err, ErrNotFound) && !errors.Is(createErr, ErrExists) {
			return fmt.Errorf("create %s: %w", ref.Key, createErr)
		}
		return err
	}
	if ref.Level == 1 {
		_, err = l.decodeSegment(ref, actual)
	} else {
		_, err = l.decodeIndex(ref, actual)
	}
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

func (l *Log[T]) decompress(body []byte) ([]byte, error) {
	if int64(len(body)) > l.maxObject {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrTooLarge)
	}
	h, err := singleFrame(body)
	if err != nil {
		return nil, err
	}
	if h.HasFCS && h.FrameContentSize > uint64(l.maxObject) {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrTooLarge)
	}
	dec, err := zstd.NewReader(bytes.NewReader(body), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(max(l.maxObject, 1<<20))), zstd.WithDecoderMaxWindow(1<<20))
	if err != nil {
		return nil, corrupt("invalid zstd stream")
	}
	defer dec.Close()
	raw, err := io.ReadAll(io.LimitReader(dec, l.maxObject+1))
	if int64(len(raw)) > l.maxObject || errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded) {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrTooLarge)
	}
	if err != nil {
		return nil, corrupt("invalid zstd data")
	}
	return raw, nil
}

func (l *Log[T]) validateSegment(ref objectRef, seg *segment) error {
	start, _, err := l.validateRef(ref)
	if err != nil {
		return err
	}
	if ref.Level != 1 || seg.Format != segmentFormat || seg.Level != 1 || seg.Start != ref.Start || seg.End != ref.End || len(seg.Records) != 16 {
		return corrupt("segment envelope mismatch")
	}
	chain := ref.FirstPrevHash
	for i, p := range seg.Records {
		revision, err := l.validateProjection(p)
		if err != nil {
			return err
		}
		if revision != start+uint64(i) || p.PreviousRecordHash != chain {
			return corrupt("segment adjacency mismatch")
		}
		chain = p.RecordHash
	}
	if chain != ref.LastRecordHash {
		return corrupt("segment hash boundary mismatch")
	}
	return nil
}

func (l *Log[T]) decodeSegment(ref objectRef, body []byte) (*segment, error) {
	raw, err := l.decompress(body)
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
	if err := l.validateSegment(ref, &seg); err != nil {
		return nil, err
	}
	return &seg, nil
}

func (l *Log[T]) decodeIndex(ref objectRef, body []byte) (*indexNode, error) {
	if int64(len(body)) > l.maxObject {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrTooLarge)
	}
	if digest(body) != ref.SHA256 {
		return nil, corrupt("index digest mismatch")
	}
	var node indexNode
	if err := decodeJSON(body, &node); err != nil {
		return nil, err
	}
	if node.Format != indexFormat || node.Level != ref.Level || node.Start != ref.Start || node.End != ref.End {
		return nil, corrupt("index envelope mismatch")
	}
	if err := l.validateChildren(ref, node.Children); err != nil {
		return nil, err
	}
	return &node, nil
}
