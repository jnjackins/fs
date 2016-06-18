package main

import (
	"syscall"
	"testing"
)

func testAllocDisk() (*Disk, error) {
	fd, err := syscall.Open(testFossilPath, syscall.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	disk, err := diskAlloc(fd)
	if err != nil {
		return nil, err
	}

	return disk, nil
}

func TestDisk(t *testing.T) {
	disk, err := testAllocDisk()
	if err != nil {
		t.Fatalf("error allocating disk: %v", err)
	}
	defer disk.free()

	t.Run("disk.readRaw", func(t *testing.T) { benchDiskReadRaw(b, disk) })
}

func BenchmarkDisk(b *testing.B) {
	disk, err := testAllocDisk()
	if err != nil {
		b.Fatalf("error allocating disk: %v", err)
	}
	defer disk.free()

	b.Run("disk.writeRaw", func(b *testing.B) { benchDiskWriteRaw(b, disk) })
	b.Run("disk.readRaw", func(b *testing.B) { benchDiskReadRaw(b, disk) })
}

func benchDiskWriteRaw(b *testing.B, disk *Disk) {
	buf := make([]byte, disk.h.blockSize)
	for i := 0; i < b.N; i++ {
		err := disk.writeRaw(PartData, disk.partStart(PartData), buf)
		if err != nil {
			b.Fatalf("disk.writeRaw: %v", err)
		}
	}
}

func benchDiskReadRaw(b *testing.B, disk *Disk) {
	buf := make([]byte, disk.h.blockSize)
	err := disk.writeRaw(PartData, disk.partStart(PartData), buf)
	if err != nil {
		b.Fatalf("disk.writeRaw: %v", err)
	}

	for i := 0; i < b.N; i++ {
		err := disk.readRaw(PartData, disk.partStart(PartData), buf)
		if err != nil {
			b.Fatalf("disk.readRaw: %v", err)
		}
	}
}
