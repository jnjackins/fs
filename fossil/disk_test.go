package main

import (
	"bytes"
	"os"
	"syscall"
	"testing"
)

func testAllocDisk() (*Disk, string, error) {
	path, err := testFormatFossil()
	if err != nil {
		return nil, "", err
	}

	fd, err := syscall.Open(path, syscall.O_RDWR, 0)
	if err != nil {
		return nil, path, err
	}

	disk, err := allocDisk(fd)
	if err != nil {
		return nil, path, err
	}

	return disk, path, nil
}

func TestDisk(t *testing.T) {
	disk, path, err := testAllocDisk()
	if err != nil {
		if path != "" {
			os.Remove(path)
		}
		t.Fatalf("error allocating disk: %v", err)
	}
	defer os.Remove(path)
	defer disk.free()

	t.Run("disk.readWriteRaw", func(t *testing.T) { testDiskReadWriteRaw(t, disk) })
}

func testDiskReadWriteRaw(t *testing.T, disk *Disk) {
	buf := make([]byte, disk.h.blockSize)
	copy(buf, []byte("the quick brown fox jumps over the lazy dog"))

	want := make([]byte, disk.h.blockSize)
	copy(want, buf)

	if err := disk.writeRaw(PartData, disk.partStart(PartData), buf); err != nil {
		t.Fatalf("disk.writeRaw: %v", err)
	}

	memset(buf, 0)

	if err := disk.readRaw(PartData, disk.partStart(PartData), buf); err != nil {
		t.Fatalf("disk.readRaw: %v", err)
	}

	if !bytes.Equal(buf, want) {
		t.Errorf("comparison failed: got=%v, want=%v", buf, want)
	}
}

func BenchmarkDisk(b *testing.B) {
	disk, path, err := testAllocDisk()
	if err != nil {
		if path != "" {
			os.Remove(path)
		}
		b.Fatalf("error allocating disk: %v", err)
	}
	defer os.Remove(path)
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
	for i := 0; i < b.N; i++ {
		err := disk.readRaw(PartData, disk.partStart(PartData), buf)
		if err != nil {
			b.Fatalf("disk.readRaw: %v", err)
		}
	}
}
