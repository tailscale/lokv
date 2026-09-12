// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package memstore_test

import (
	"github.com/tailscale/lokv/memstore"
	"github.com/tailscale/lokv/storetest"
	"testing"
)

func TestConformance(t *testing.T) { storetest.Test(t, new(memstore.Store), "test/") }
