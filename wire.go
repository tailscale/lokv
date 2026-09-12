// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

const (
	commitFormat  = "lokv/commit/v1"
	segmentFormat = "lokv/segment/v1"
	indexFormat   = "lokv/index/v1"
	zeroHash      = "0000000000000000000000000000000000000000000000000000000000000000"
)

type previous struct {
	Key        string `json:"key"`
	RecordHash string `json:"record_hash"`
}
type objectRef struct {
	Level          uint8  `json:"level"`
	Start          string `json:"start"`
	End            string `json:"end"`
	Key            string `json:"key"`
	SHA256         string `json:"sha256"`
	FirstPrevHash  string `json:"first_prev_hash"`
	LastRecordHash string `json:"last_record_hash"`
}
type frontierLevel struct {
	Level uint8       `json:"level"`
	Refs  []objectRef `json:"refs"`
}
type commit struct {
	Format     string          `json:"format"`
	Revision   string          `json:"revision"`
	CommitID   string          `json:"commit_id"`
	Previous   *previous       `json:"previous"`
	Event      json.RawMessage `json:"event"`
	RecordHash string          `json:"record_hash"`
	Frontier   []frontierLevel `json:"frontier"`
}
type projection struct {
	Revision           string          `json:"revision"`
	CommitID           string          `json:"commit_id"`
	PreviousRecordHash string          `json:"previous_record_hash"`
	RecordHash         string          `json:"record_hash"`
	Event              json.RawMessage `json:"event"`
}
type segment struct {
	Format  string       `json:"format"`
	Level   uint8        `json:"level"`
	Start   string       `json:"start"`
	End     string       `json:"end"`
	Records []projection `json:"records"`
}
type indexNode struct {
	Format   string      `json:"format"`
	Level    uint8       `json:"level"`
	Start    string      `json:"start"`
	End      string      `json:"end"`
	Children []objectRef `json:"children"`
}

func hexRevision(r int64) string { return fmt.Sprintf("%016x", r) }

func validHex(s string, n int) (valid bool) {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// parseHexInt64 also accepts zero, which is a valid reverse-encoded key suffix.
func parseHexInt64(s string) (int64, error) {
	if !validHex(s, 16) {
		return 0, corrupt("invalid revision")
	}
	r, err := strconv.ParseInt(s, 16, 64)
	if err != nil {
		return 0, corrupt("revision exceeds int64 range")
	}
	return r, nil
}

func parseRevision(s string) (int64, error) {
	r, err := parseHexInt64(s)
	if err != nil {
		return 0, err
	}
	if r <= 0 {
		return 0, corrupt("revision must be positive")
	}
	return r, nil
}

func corrupt(message string) error  { return fmt.Errorf("%w: %s", ErrCorrupt, message) }
func digest(b []byte) string        { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (l *Log[T]) logPrefix() string { return l.prefix + "v1/log/" }

// logKey requires a positive revision, checked by callers before encoding.
func (l *Log[T]) logKey(r int64) string {
	return l.logPrefix() + hexRevision(math.MaxInt64-r) + ".json"
}

func (l *Log[T]) parseLogKey(key string) (int64, error) {
	if !strings.HasPrefix(key, l.logPrefix()) {
		return 0, corrupt("foreign commit key")
	}
	s := strings.TrimPrefix(key, l.logPrefix())
	if len(s) != 21 || !strings.HasSuffix(s, ".json") {
		return 0, corrupt("invalid commit key")
	}
	r, err := parseHexInt64(s[:16])
	if err != nil {
		return 0, err
	}
	if r == math.MaxInt64 {
		return 0, corrupt("commit key encodes revision zero")
	}
	return math.MaxInt64 - r, nil
}

func (l *Log[T]) treeKey(ref objectRef) string {
	suffix := ".json"
	if ref.Level == 1 {
		suffix += ".zst"
	}
	return fmt.Sprintf("%sv1/tree/%x/%s-%s-%s%s", l.prefix, ref.Level, ref.Start, ref.End, ref.SHA256, suffix)
}

func recordHash(r int64, id, prev string, event []byte) string {
	h := sha256.New()
	h.Write([]byte("lokv-record-v1\x00"))
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(r))
	h.Write(n[:])
	b, _ := hex.DecodeString(id)
	h.Write(b)
	b, _ = hex.DecodeString(prev)
	h.Write(b)
	binary.BigEndian.PutUint64(n[:], uint64(len(event)))
	h.Write(n[:])
	h.Write(event)
	return hex.EncodeToString(h.Sum(nil))
}

func (c *commit) project() projection {
	prev := zeroHash
	if c.Previous != nil {
		prev = c.Previous.RecordHash
	}
	return projection{c.Revision, c.CommitID, prev, c.RecordHash, c.Event}
}

// Check required fields separately: Go's decoder otherwise accepts absent or
// null numeric fields as zero. Unknown fields are allowed for future extensions.
func decodeJSON(b []byte, dst any) error {
	if err := requiredFields(b, reflect.TypeOf(dst).Elem()); err != nil {
		return err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return corrupt("invalid JSON envelope")
	}
	return nil
}

var rawMessageType = reflect.TypeFor[json.RawMessage]()

func requiredFields(b []byte, t reflect.Type) error {
	if t == rawMessageType {
		if !json.Valid(b) {
			return corrupt("invalid event JSON")
		}
		return nil
	}
	if t.Kind() == reflect.Pointer {
		if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
			return nil
		}
		return requiredFields(b, t.Elem())
	}
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return corrupt("null required field")
	}
	switch t.Kind() {
	case reflect.Struct:
		// Token decoding also rejects duplicate names, avoiding ambiguous envelopes.
		d := json.NewDecoder(bytes.NewReader(b))
		tok, err := d.Token()
		if err != nil || tok != json.Delim('{') {
			return corrupt("invalid JSON object")
		}
		fields := make(map[string]json.RawMessage)
		for d.More() {
			tok, err = d.Token()
			if err != nil {
				return corrupt("invalid JSON field")
			}
			name, ok := tok.(string)
			if !ok {
				return corrupt("invalid JSON field name")
			}
			if _, ok := fields[name]; ok {
				return corrupt("duplicate JSON field")
			}
			var raw json.RawMessage
			if err := d.Decode(&raw); err != nil {
				return corrupt("invalid JSON field value")
			}
			fields[name] = raw
		}
		if _, err := d.Token(); err != nil {
			return corrupt("invalid JSON object ending")
		}
		if !json.Valid(b) {
			return corrupt("invalid or trailing JSON")
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := f.Tag.Get("json")
			for supplied := range fields {
				if supplied != name && strings.EqualFold(supplied, name) {
					return corrupt("noncanonical JSON field name")
				}
			}
			raw, ok := fields[name]
			if !ok {
				return corrupt("missing field " + name)
			}
			if err := requiredFields(raw, f.Type); err != nil {
				return err
			}
		}
	case reflect.Slice:
		var a []json.RawMessage
		if err := json.Unmarshal(b, &a); err != nil {
			return corrupt("invalid JSON array")
		}
		for _, raw := range a {
			if err := requiredFields(raw, t.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *Log[T]) validateProjection(p projection) (int64, error) {
	r, err := parseRevision(p.Revision)
	if err != nil {
		return 0, err
	}
	if !validHex(p.CommitID, 32) || !validHex(p.PreviousRecordHash, 64) || !validHex(p.RecordHash, 64) {
		return 0, corrupt("invalid record hash or ID")
	}
	if r == 1 && p.PreviousRecordHash != zeroHash {
		return 0, corrupt("invalid genesis hash")
	}
	if int64(len(p.Event)) > l.maxEvent {
		return 0, fmt.Errorf("%w: %w: event", ErrCorrupt, ErrTooLarge)
	}
	// RawMessage marshaling compacts and HTML-escapes its input. Requiring an
	// event to be stable under this operation ensures packing never changes the
	// authoritative bytes. Every json.Marshal(value) result has this property.
	canonical, err := json.Marshal(p.Event)
	if err != nil || !bytes.Equal(canonical, p.Event) {
		return 0, corrupt("noncanonical event JSON")
	}
	if recordHash(r, p.CommitID, p.PreviousRecordHash, p.Event) != p.RecordHash {
		return 0, corrupt("record hash mismatch")
	}
	return r, nil
}

func (l *Log[T]) validateRef(ref objectRef) (int64, int64, error) {
	if ref.Level > 15 {
		return 0, 0, corrupt("invalid reference level")
	}
	start, err := parseRevision(ref.Start)
	if err != nil {
		return 0, 0, err
	}
	end, err := parseRevision(ref.End)
	if err != nil {
		return 0, 0, err
	}
	size := int64(1) << (4 * ref.Level)
	if (start-1)%size != 0 || start > math.MaxInt64-(size-1) || end != start+(size-1) {
		return 0, 0, corrupt("invalid reference range")
	}
	if !validHex(ref.SHA256, 64) || !validHex(ref.FirstPrevHash, 64) || !validHex(ref.LastRecordHash, 64) {
		return 0, 0, corrupt("invalid reference digest")
	}
	key := l.treeKey(ref)
	if ref.Level == 0 {
		key = l.logKey(start)
	}
	if ref.Key != key {
		return 0, 0, corrupt("reference key mismatch")
	}
	return start, end, nil
}

func (l *Log[T]) validateFrontier(frontier []frontierLevel, revision int64, prevHash string) error {
	if revision <= 0 {
		return corrupt("revision must be positive")
	}
	var levels [16][]objectRef
	last := -1
	for _, f := range frontier {
		if int(f.Level) <= last || f.Level > 15 || len(f.Refs) == 0 || len(f.Refs) > 15 {
			return corrupt("noncanonical frontier levels")
		}
		last = int(f.Level)
		levels[f.Level] = f.Refs
	}
	next, chain := int64(1), zeroHash
	for level := 15; level >= 0; level-- {
		refs := levels[level]
		if len(refs) != int(((revision-1)>>(4*level))&15) {
			return corrupt("frontier digit mismatch")
		}
		for _, ref := range refs {
			start, end, err := l.validateRef(ref)
			if err != nil {
				return err
			}
			if int(ref.Level) != level || start != next || end >= revision || ref.FirstPrevHash != chain {
				return corrupt("frontier partition or chain mismatch")
			}
			next, chain = end+1, ref.LastRecordHash
		}
	}
	if next != revision || chain != prevHash {
		return corrupt("frontier boundary mismatch")
	}
	return nil
}

func (l *Log[T]) decodeCommit(key string, body []byte) (*commit, error) {
	if int64(len(body)) > l.maxObject {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrTooLarge)
	}
	r, err := l.parseLogKey(key)
	if err != nil {
		return nil, err
	}
	var c commit
	if err := decodeJSON(body, &c); err != nil {
		return nil, err
	}
	if c.Format != commitFormat || c.Revision != hexRevision(r) {
		return nil, corrupt("commit format or revision mismatch")
	}
	if r == 1 {
		if c.Previous != nil {
			return nil, corrupt("genesis has predecessor")
		}
	} else if c.Previous == nil || c.Previous.Key != l.logKey(r-1) {
		return nil, corrupt("invalid predecessor key")
	}
	p := c.project()
	if _, err := l.validateProjection(p); err != nil {
		return nil, err
	}
	if err := l.validateFrontier(c.Frontier, r, p.PreviousRecordHash); err != nil {
		return nil, err
	}
	return &c, nil
}
