package main

import (
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/tysonmote/gommap"
)

const (
	increment = 131072
	initial   = 256
)

//MemFile 内存文件
type MemFile struct {
	File *os.File
	Map  gommap.MMap
	Len  int64 //原子性
	Off  int64
	Tree uint64
}

//MemFileOpen 打开内存文件
func MemFileOpen(file *os.File) *MemFile {
	if file == nil {
		return nil
	}
	err := file.Truncate(initial)
	if err != nil {
		log.Error(err)
		return nil
	}
	mmap, err := gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		log.Error(err)
		return nil
	}
	mf := &MemFile{}
	mf.Map = mmap
	mf.Len = initial
	mf.Off = 0
	mf.Tree = 0
	return mf
}

// MemFileClose 关闭内存文件
func MemFileClose(memfile *MemFile) error {
	err := memfile.Map.UnsafeUnmap()
	if err != nil {
		return err
	}
	err = memfile.File.Close()
	if err != nil {
		return err
	}
	return nil
}

//MemFileWrite 写内存文件
func MemFileWrite(mf *MemFile, s []byte) int {
	//内容过大，增加文件大小
	// Len := int64(unsafe.Sizeof(s))
	Len := int64(len(s))
	if mf.Off+Len > mf.Len {
		if err := mf.Map.UnsafeUnmap(); err != nil {
			log.Error(err)
			return -1
		}

		mf.Len += (Len + increment + 1) / increment * increment
		if err := mf.File.Truncate(mf.Len); err != nil {
			log.Error(err)
			return -1
		}

		mmap, err := gommap.Map(mf.File.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
		if err != nil {
			log.Error(err)
			return -1
		}

		mf.Map = mmap
	}
	// sbuf := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&s)))[:Len]
	copy(mf.Map[mf.Off:], s)
	mf.Off += Len
	return int(Len)
}
