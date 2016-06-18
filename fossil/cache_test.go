package main

import "testing"

func testAllocCache() (*Cache, error) {
	disk, err := testAllocDisk()
	if err != nil {
		return nil, err
	}
	return allocCache(disk, nil, 100, OReadWrite), nil
}

func TestCache(t *testing.T) {
	cache, err := testAllocCache()
	if err != nil {
		t.Fatalf("error allocating cache: %v", err)
	}
	defer cache.free()

	t.Run("cache.local", func(t *testing.T) { testCacheLocal(t, cache) })
}

func testCacheLocal(t *testing.T, c *Cache) {
	b, err := c.local(PartData, c.disk.partStart(PartData), OReadWrite)
	if err != nil {
		t.Fatalf("cache.local: %v", err)
	}
	b.put()
}
