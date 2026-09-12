// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"context"
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
