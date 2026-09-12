// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package lokv

import (
	"bytes"
	"context"
	"io"
)

// Only tests materialize whole segments to construct and mutate fixtures.
type segment struct {
	Format  string       `json:"format"`
	Level   uint8        `json:"level"`
	Start   string       `json:"start"`
	End     string       `json:"end"`
	Records []projection `json:"records"`
}

func (lg *Log[T]) decompressBytes(body []byte) ([]byte, error) {
	dec, err := lg.decompress(context.Background(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return io.ReadAll(dec)
}
