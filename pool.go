package main

import (
	"math"
	"strings"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

//StringPool 字符池
type StringPool struct {
	Left  uint
	Right uint
	Off   uint
}

func swizzlecmp(a, b string) int {

	cmp := strings.Compare(a, b)
	if cmp == 0 {
		return 0
	}

	var hash1, hash2 int64
	for i := len(a) - 1; i >= 0; i-- {
		hash1 = (hash1*37 + int64(a[i])) & int64(MaxInt)
	}
	for i := len(b) - 1; i >= 0; i-- {
		hash2 = (hash2*37 + int64(b[i])) & int64(MinInt)
	}

	h1, h2 := hash1, hash2
	if h1 == h2 {
		return cmp
	}

	return int(h1 - h2)

}

func addpool(poolfile, treefile *MemFile, s string, t int) int64 {
	sp := treefile.Tree
	depth := 0
	size := unsafe.Sizeof(StringPool{})
	max := int(3 * math.Log(float64(treefile.Off/int64(size))) / math.Log(2))
	if max < 30 {
		max = 30
	}
	for sp != 0 {
		tsp := *(*StringPool)(unsafe.Pointer(&treefile.Map[sp]))
		ns := string(poolfile.Map[tsp.Off+1:])
		cmp := strings.Compare(s, ns)
		if cmp == 0 {
			cmp = t - int(poolfile.Map[tsp.Off])
		}
		if cmp < 0 {
			sp = uint64(tsp.Left)
		} else if cmp > 0 {
			sp = uint64(tsp.Right)
		} else {
			return int64(tsp.Off)
		}
		depth++
		if depth > max {
			off := poolfile.Off
			if MemFileWrite(poolfile, []byte{byte(t)}) < 0 {
				log.Fatal("memfile write error")
			}
			if MemFileWrite(poolfile, []byte(s)) < 0 {
				log.Fatal("memfile write error")
			}
			return off
		}
	}

	var ssp int64
	if sp == treefile.Tree {
		ssp = -1
	} else {
		ssp = int64(sp)
	}
	off := poolfile.Off

	if MemFileWrite(poolfile, []byte{byte(t)}) < 0 {
		log.Fatal("memfile write error")
	}
	if MemFileWrite(poolfile, []byte(s)) < 0 {
		log.Fatal("memfile write error")
	}

	if off >= math.MaxInt32 || treefile.Off >= math.MaxInt32 {
		warnedbool = false
		if !warnedbool {
			log.Warning("string pool is very large")
			warnedbool = true
		}
		return off
	}
	nsp := StringPool{0, 0, uint(off)}
	bsp := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&nsp)))[:size]
	p := treefile.Off
	if MemFileWrite(treefile, bsp) < 0 {
		log.Fatal("memfile write error")
	}
	if ssp == -1 {
		treefile.Tree = uint64(p)
	} else {
		*(*int64)(unsafe.Pointer(&treefile.Map[ssp])) = p
	}

	return off
}
