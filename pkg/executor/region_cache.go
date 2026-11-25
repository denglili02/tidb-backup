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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikv"
)

const planCacheRegionRefreshBackoff = 5000

type regionCacheProvider interface {
	GetRegionCache() *tikv.RegionCache
}

// refreshRegionCacheForPlanCacheHit refreshes the shared client routing for
// point-read keys. PointGet and BatchPointGet bypass kv.Request and the
// coprocessor client, so their cache-hit path must refresh through the storage
// region cache before snapshot Get or BatchGet routes the keys.
func refreshRegionCacheForPlanCacheHit(ctx context.Context, store kv.Storage, keys []kv.Key) error {
	provider, ok := store.(regionCacheProvider)
	if !ok {
		return nil
	}
	return refreshRegionCacheForKeys(ctx, provider.GetRegionCache(), keys)
}

func refreshRegionCacheForKeys(ctx context.Context, cache *tikv.RegionCache, keys []kv.Key) error {
	if cache == nil || len(keys) == 0 {
		return nil
	}
	bo := tikv.NewBackoffer(ctx, planCacheRegionRefreshBackoff)
	invalidated := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		loc, err := cache.LocateKey(bo, key)
		if err != nil {
			return err
		}
		if _, ok := invalidated[loc.Region.GetID()]; !ok {
			cache.InvalidateCachedRegion(loc.Region)
			invalidated[loc.Region.GetID()] = struct{}{}
		}
		if _, err = cache.LocateKey(bo, key); err != nil {
			return err
		}
	}
	return nil
}
