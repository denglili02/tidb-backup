// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRefreshRegionCacheForPointGetCacheHit(t *testing.T) {
	cache, cluster, regionIDs, storeID, closeCache := newStaleRegionCacheForPointRead(t)
	defer closeCache()

	newRegionID := cluster.AllocID()
	newPeerID := cluster.AllocID()
	cluster.Split(regionIDs[1], newRegionID, []byte("q"), []uint64{newPeerID}, storeID)

	bo := tikv.NewBackoffer(context.Background(), planCacheRegionRefreshBackoff)
	staleLoc, err := cache.LocateKey(bo, []byte("n"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], staleLoc.Region.GetID())

	// A point-get cache hit refreshes its one concrete key before snapshot.Get.
	require.NoError(t, refreshRegionCacheForKeys(context.Background(), cache, []kv.Key{[]byte("n")}))
	loc, err := cache.LocateKey(bo, []byte("n"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], loc.Region.GetID())
	require.Equal(t, []byte("q"), loc.EndKey)
}

func TestRefreshRegionCacheForPointGetWithoutPlanCacheHit(t *testing.T) {
	cache, cluster, regionIDs, storeID, closeCache := newStaleRegionCacheForPointRead(t)
	defer closeCache()

	newRegionID := cluster.AllocID()
	newPeerID := cluster.AllocID()
	cluster.Split(regionIDs[1], newRegionID, []byte("q"), []uint64{newPeerID}, storeID)

	// Non-cache-hit execution must leave the shared cache alone. In particular,
	// this stale mapping must not be invalidated or reloaded just because a
	// point read is executed.
	require.NoError(t, refreshRegionCacheForKeysIfPlanCacheHit(context.Background(), cache, false, []kv.Key{[]byte("n")}))
	bo := tikv.NewBackoffer(context.Background(), planCacheRegionRefreshBackoff)
	loc, err := cache.LocateKey(bo, []byte("n"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], loc.Region.GetID())
	require.Empty(t, loc.EndKey)
}

func TestRefreshRegionCacheForBatchPointGetCacheHit(t *testing.T) {
	cache, cluster, regionIDs, storeID, closeCache := newStaleRegionCacheForPointRead(t)
	defer closeCache()

	newRegionID := cluster.AllocID()
	newPeerID := cluster.AllocID()
	cluster.Split(regionIDs[1], newRegionID, []byte("q"), []uint64{newPeerID}, storeID)

	bo := tikv.NewBackoffer(context.Background(), planCacheRegionRefreshBackoff)
	staleLoc, err := cache.LocateKey(bo, []byte("r"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], staleLoc.Region.GetID())

	// A batch point-get may span both children of the split region. Refresh all
	// concrete keys before BatchGet groups them into region requests.
	require.NoError(t, refreshRegionCacheForKeys(context.Background(), cache, []kv.Key{[]byte("n"), []byte("r")}))
	left, err := cache.LocateKey(bo, []byte("n"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], left.Region.GetID())
	require.Equal(t, []byte("q"), left.EndKey)
	right, err := cache.LocateKey(bo, []byte("r"))
	require.NoError(t, err)
	require.Equal(t, newRegionID, right.Region.GetID())
	require.Equal(t, []byte("q"), right.StartKey)
}

func TestRefreshRegionCacheForBatchPointGetIndexAndRowKeys(t *testing.T) {
	cache, cluster, regionIDs, storeID, closeCache := newStaleRegionCacheForPointRead(t)
	defer closeCache()

	newRegionID := cluster.AllocID()
	newPeerID := cluster.AllocID()
	cluster.Split(regionIDs[1], newRegionID, []byte("q"), []uint64{newPeerID}, storeID)

	// A unique-index BatchPointGet issues two snapshot BatchGet calls: first
	// for index keys and then for the row keys decoded from their values. Both
	// calls must refresh before they are grouped by region.
	require.NoError(t, refreshRegionCacheForKeys(context.Background(), cache, []kv.Key{[]byte("n")}))
	require.NoError(t, refreshRegionCacheForKeys(context.Background(), cache, []kv.Key{[]byte("r")}))

	bo := tikv.NewBackoffer(context.Background(), planCacheRegionRefreshBackoff)
	indexLoc, err := cache.LocateKey(bo, []byte("n"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], indexLoc.Region.GetID())
	rowLoc, err := cache.LocateKey(bo, []byte("r"))
	require.NoError(t, err)
	require.Equal(t, newRegionID, rowLoc.Region.GetID())
}

func newStaleRegionCacheForPointRead(t *testing.T) (*tikv.RegionCache, testutils.Cluster, []uint64, uint64, func()) {
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	storeID, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("m"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	cache := tikv.NewRegionCache(pdCli)

	// Warm the client cache while [m, +inf) is still one region.
	bo := tikv.NewBackoffer(context.Background(), planCacheRegionRefreshBackoff)
	loc, err := cache.LocateKey(bo, []byte("n"))
	require.NoError(t, err)
	require.Equal(t, regionIDs[1], loc.Region.GetID())

	return cache, cluster, regionIDs, storeID, func() {
		cache.Close()
		pdCli.Close()
		pdClient.Close()
		require.NoError(t, mockClient.Close())
	}
}
