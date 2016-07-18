package main

import (
	"os"
	"testing"
)

func TestCache(t *testing.T) {
	disk, path, err := testAllocDisk()
	if err != nil {
		if path != "" {
			os.Remove(path)
		}
		t.Fatalf("error allocating disk: %v", err)
	}
	defer os.Remove(path)

	cache := allocCache(disk, nil, 100, OReadWrite)
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
