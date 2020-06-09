package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

//CoordOffset 坐标偏移量（使坐标值保持正值）
const CoordOffset int64 = 4 << 32

var warnedint int64
var warnedbool bool

//ShiftRight 右移
func ShiftRight(a int64) int64 {
	return ((a + CoordOffset) >> geometryScale) - (CoordOffset >> geometryScale)
}

//ShiftLeft 左移
func ShiftLeft(a int64) int64 {
	return ((a + (CoordOffset >> geometryScale)) << geometryScale) - CoordOffset
}

//SerializeByte 序列化单字节
func SerializeByte(out *os.File, b byte, fpos *int64) {
	x := []byte{b}
	s, err := out.Write(x)
	if err != nil {
		log.Fatal(err)
	}
	nfpos := atomic.AddInt64(fpos, int64(s))

	if nfpos != *fpos {
		log.Fatal("the fpos added error")
	}
}

//SerializeUint 序列化单字节
// func SerializeUint(out *os.File, n uint, fpos *int64) {
// 	// x := []byte{n}
// 	s, err := out.Write(x)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	nfpos := atomic.AddInt64(fpos, int64(s))

// 	if nfpos != *fpos {
// 		log.Fatal("the fpos added error")
// 	}
// }

//SerialVal 序列化值
type SerialVal struct {
	Type int
	S    string
}

//SerialFeature 序列化要素
type SerialFeature struct {
	Layer   int
	Segment int
	Seq     int64

	T              int8
	FeatureMinzoom int8

	HasID bool
	ID    int64

	HasMinzoom bool
	Minzoom    int

	HasMaxzoom bool
	Maxzoom    int

	geometry DrawVec //[]Draw
	Index    uint64
	Extent   int64

	Keys   []int64
	Values []int64
	// If >= 0, metadata is external
	Metapos int64

	// XXX This isn't serialized. Should it be here?
	BBox       [4]int64
	FullKeys   []string
	FullValues []SerialVal
	LayerName  string
	Dropped    bool
}

//Reader 读写器
type Reader struct {
	Metafd  int
	Geomfd  int
	Indexfd int
	Poolfd  int
	Treefd  int

	Metafile  *os.File
	Geomfile  *os.File
	Indexfile *os.File

	Poolfile *os.File
	Treefile *os.File

	PoolMemFile *MemFile
	TreeMemFile *MemFile

	Metapos  int64 //atomic
	Geompos  int64 //atomic
	Indexpos int64 //atomic

	FileBBox [4]int64

	Geomst os.FileInfo
	Metast os.FileInfo

	GeomMap *byte
}

//SerializationState 序列状态结构体
type SerializationState struct {
	FName string // source file name(const char*)
	Line  int    // user-oriented location within source for error reports

	LayerSeq *int64 // sequence within current layer//当前图层的序列
	// std::atomic<long long> *layer_seq = NULL;
	ProgresSeq *int64 // overall sequence for progress indicator//进度指示器
	// std::atomic<long long> *progress_seq = NULL;
	//每个输入进程的数据数组
	Readers []Reader
	// std::vector<struct reader> *readers = NULL;  // array of data for each input thread
	Segment int // the current input thread

	//相对偏移量
	InitialX    *uint // relative offset of all geometries
	InitialY    *uint
	Initialized *int

	DistSum   *float64 // running tally for calculation of resolution within features
	DistCount *int64
	WantDist  bool

	Maxzoom  int
	Basezoom int

	Filters        bool
	UsesGamma      bool
	LayerMap       map[string]LayerEntry
	AttributeTypes map[string]int
	Exclude        []string
	Include        []string
	ExcludeAll     bool
}

//SerializeFeature 序列化要素
func SerializeFeature(sst *SerializationState, sf *SerialFeature) int {
	r := sst.Readers[sst.Segment]
	sf.BBox[0] = math.MaxInt64
	sf.BBox[1] = math.MaxInt64
	sf.BBox[2] = math.MinInt64
	sf.BBox[3] = math.MinInt64
	fmt.Println(r)
	ScaleGeometry(sst, sf.BBox[:], sf.geometry)
	if sf.T == vtPolygon {
		sf.geometry = FixPolygon(sf.geometry)
	}
	for _, cb := range clipboxes {
		fmt.Println(cb)
		if sf.T == vtPolygon {
		} else if sf.T == vtLine {
		} else if sf.T == vtPoint {
		}
		sf.BBox[0] = math.MaxInt64
		sf.BBox[1] = math.MaxInt64
		sf.BBox[2] = math.MinInt64
		sf.BBox[3] = math.MinInt64
		for _, g := range sf.geometry {
			x := ShiftLeft(g.X)
			y := ShiftLeft(g.Y)

			if x < sf.BBox[0] {
				sf.BBox[0] = x
			}
			if y < sf.BBox[1] {
				sf.BBox[1] = y
			}
			if x > sf.BBox[2] {
				sf.BBox[2] = x
			}
			if y > sf.BBox[3] {
				sf.BBox[3] = y
			}
		}
	}

	if len(sf.geometry) == 0 {
		return 1
	}

	if !sf.HasID {
		if additional[aGenerateIDs] != 0 {
			sf.HasID = true
			sf.ID = sf.Seq + 1
		}
	}

	if sst.WantDist {
		locs := []uint64{}
		for _, g := range sf.geometry {
			if g.Opt == vtMoveto || g.Opt == vtLineto {
				idx := EncodeQuadkey(uint64(ShiftLeft(g.X)), uint64(ShiftLeft(g.Y)))
				locs = append(locs, idx)
			}
		}

		sort.Slice(locs, func(i, j int) bool { return locs[i] < locs[j] })
		var n int64
		var sum float64
		for i := 1; i < len(locs); i++ {
			if locs[i-1] != locs[i] {
				sum += math.Log(float64(locs[i] - locs[i-1]))
				n++
			}
		}
		if n > 0 {

			avg := math.Exp(sum / float64(n))
			dist := math.Sqrt(avg) / 33.0
			*(sst.DistSum) += math.Log(dist) * float64(n)
			*(sst.DistCount) += n
		}
	}
	inlineMeta := true
	if len(sf.geometry) > 0 && (sf.BBox[2] < sf.BBox[0] || sf.BBox[3] < sf.BBox[1]) {
		log.Errorf("Internal error: impossible feature bounding box %v\n", sf.BBox)
	}

	if sf.BBox[2]-sf.BBox[0] > (2<<uint(32-sst.Maxzoom)) || sf.BBox[3]-sf.BBox[1] > (2<<uint(32-sst.Maxzoom)) {
		inlineMeta = false
		if prevent[pClipping] != 0 {
			extent := ((sf.BBox[2] - sf.BBox[0]) / ((1 << uint(32-sst.Maxzoom)) + 1)) * ((sf.BBox[3] - sf.BBox[1]) / ((1 << uint(32-sst.Maxzoom)) + 1))
			if extent > warnedint {
				log.Warningf("Warning: Large unclipped (-pc) feature may be duplicated across %d tiles\n", extent)
				atomic.StoreInt64(&warnedint, extent)
				if extent > 10000 {
					log.Fatal("Exiting because this can't be right.\n")
				}
			}
		}
	}
	fmt.Println(inlineMeta)

	var extent float64
	if additional[aDropSmallestAsNeeded] != 0 || additional[aCoalesceSmallestAsNeeded] != 0 {
		if sf.T == vtPolygon {
			for i, g := range sf.geometry {
				if g.Opt == vtMoveto {
					var j int
					for j = i + 1; j < len(sf.geometry); j++ {
						if sf.geometry[j].Opt != vtLineto {
							break
						}
					}
					extent += GetArea(sf.geometry, i, j)
					i = j - 1
				}
			}
		} else if sf.T == vtLine {
			for i := 1; i < len(sf.geometry); i++ {
				if sf.geometry[i].Opt == vtLineto {
					xd := sf.geometry[i].X - sf.geometry[i-1].X
					yd := sf.geometry[i].Y - sf.geometry[i-1].Y
					extent += math.Sqrt(float64(xd*xd + yd*yd))
				}
			}
		}
	}
	if extent <= math.MaxInt64 {
		sf.Extent = int64(extent)
	} else {
		sf.Extent = math.MaxInt64
	}
	if prevent[pInputOrder] == 0 {
		sf.Seq = 0
	}

	// Calculate the center even if off the edge of the plane,
	// and then mask to bring it back into the addressable area
	midx := (sf.BBox[0]/2 + sf.BBox[2]/2) & ((1 << 32) - 1)
	midy := (sf.BBox[1]/2 + sf.BBox[3]/2) & ((1 << 32) - 1)
	bboxIndex := EncodeQuadkey(uint64(midx), uint64(midy))

	if additional[aDropDensestAsNeeded] != 0 || additional[aCoalesceDensestAsNeeded] != 0 ||
		additional[aClusterDensestAsNeeded] != 0 || additional[aCalculateFeatureDensity] != 0 ||
		additional[aIncreaseGammaAsNeeded] != 0 || sst.UsesGamma || clusterDistance != 0 {
		sf.Index = bboxIndex
	} else {
		sf.Index = 0
	}

	if _, ok := sst.LayerMap[sf.LayerName]; !ok {
		sst.LayerMap[sf.LayerName] = LayerEntry{ID: len(sst.LayerMap)}
	}

	if e, ok := sst.LayerMap[sf.LayerName]; ok {
		sf.Layer = e.ID
		if !sst.Filters {
			if sf.T == vtPoint {
				e.Points++
			} else if sf.T == vtLine {
				e.Lines++
			} else if sf.T == vtPolygon {
				e.Polygons++
			}
		}
	} else {
		log.Fatalf("Internal error: can't find layer name %s\n", sf.LayerName)
	}

	for i := len(sf.FullKeys) - 1; i >= 0; i++ {
		CoerceValue(sf.FullKeys[i], &sf.FullValues[i].Type, &sf.FullValues[i].S, sst.AttributeTypes)

		if sf.FullKeys[i] == attributeForID {
			if sf.FullValues[i].Type != mvtDouble && additional[aConvertNumericIDs] == 0 {
				warnedbool = false
				if !warnedbool {
					log.Warningf("Warning: Attribute \"%s\"=\"%s\" as feature ID is not a number\n", sf.FullKeys[i], sf.FullValues[i].S)
					warnedbool = true
				}
			} else {
				id, err := strconv.ParseInt(sf.FullValues[i].S, 10, 64)
				if err != nil {
					warnedbool = false
					if !warnedbool {
						log.Warningf("Warning: Can't represent non-integer feature ID %s\n", sf.FullValues[i].S)
						warnedbool = true
					}
				} else if strconv.FormatInt(id, 10) != strings.TrimLeft(sf.FullValues[i].S, "0") {
					warnedbool = false
					if !warnedbool {
						log.Warningf("Warning: Can't represent too-large feature ID %s\n", sf.FullValues[i].S)
						warnedbool = true
					}
				} else {
					sf.ID = id
					sf.HasID = true
					sf.FullKeys = append(sf.FullKeys[:i], sf.FullKeys[i+1:]...)
					sf.FullValues = append(sf.FullValues[:i], sf.FullValues[i+1:]...)
					continue
				}
			}
		}

		if sst.ExcludeAll {
			//find should make a map for search
			find := false
			for _, v := range sst.Include {
				if v == sf.FullKeys[i] {
					find = true
					break
				}
			}
			if find {
				sf.FullKeys = append(sf.FullKeys[:i], sf.FullKeys[i+1:]...)
				sf.FullValues = append(sf.FullValues[:i], sf.FullValues[i+1:]...)
				continue
			}
		} else {
			//find should make a map for search
			find := false
			for _, v := range sst.Exclude {
				if v == sf.FullKeys[i] {
					find = true
					break
				}
			}
			if find {
				sf.FullKeys = append(sf.FullKeys[:i], sf.FullKeys[i+1:]...)
				sf.FullValues = append(sf.FullValues[:i], sf.FullValues[i+1:]...)
				continue
			}
		}
	}

	if !sst.Filters {

		for i := range sf.FullKeys {
			attrib := TypeAndString{sf.FullValues[i].Type, sf.FullValues[i].S}
			if le, ok := sst.LayerMap[sf.LayerName]; ok {
				AddToFileKeys(le.FileKeys, sf.FullKeys[i], attrib)
			}
		}
	}
	if inlineMeta {
		sf.Metapos = -1
		for i := range sf.FullKeys {
			sf.Keys = append(sf.Keys, addpool(r.PoolMemFile, r.TreeMemFile, sf.FullKeys[i], mvtString))
			sf.Values = append(sf.Values, addpool(r.PoolMemFile, r.TreeMemFile, sf.FullValues[i].S, sf.FullValues[i].Type))
		}
	} else {
		sf.Metapos = r.Metapos
		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutVarint(buf, int64(len(sf.FullKeys)))
		n, err := r.Metafile.Write(buf)
		if err != nil {
			log.Fatal("write metafile error")
		}
		atomic.AddInt64(&r.Metapos, int64(n))
		for i := range sf.FullKeys {
			binary.PutVarint(buf, addpool(r.PoolMemFile, r.TreeMemFile, sf.FullKeys[i], mvtString))
			n, err := r.Metafile.Write(buf)
			if err != nil {
				log.Fatal("write metafile error")
			}
			atomic.AddInt64(&r.Metapos, int64(n))
			binary.PutVarint(buf, addpool(r.PoolMemFile, r.TreeMemFile, sf.FullValues[i].S, sf.FullValues[i].Type))
			n, err = r.Metafile.Write(buf)
			if err != nil {
				log.Fatal("write metafile error")
			}
			atomic.AddInt64(&r.Metapos, int64(n))
		}
	}

	geomStart := r.Geompos
	SerializeFeatureGeom(r.Geomfile, sf, &r.Geompos, ShiftRight(int64(*sst.InitialX)), ShiftRight(int64(*sst.InitialY)), false)

	index := Index{}
	index.Start = geomStart
	index.End = r.Geompos
	index.Segment = sst.Segment
	index.Seq = *sst.LayerSeq
	index.T = sf.T
	index.Ix = bboxIndex
	s := unsafe.Sizeof(index)
	idxbuf := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&index)))[:s]
	n, err := r.Indexfile.Write(idxbuf)
	if err != nil {
		log.Fatal(err)
	}
	if uintptr(n) != s {
		log.Fatal("write buf size error")
	}

	for i := 0; i < 2; i++ {
		if sf.BBox[i] < r.FileBBox[i] {
			r.FileBBox[i] = sf.BBox[i]
		}
	}
	for i := 2; i < 4; i++ {
		if sf.BBox[i] > r.FileBBox[i] {
			r.FileBBox[i] = sf.BBox[i]
		}
	}
	if (*sst.ProgresSeq)%10000 == 0 {
		CheckDisk(sst.Readers)
		if !quiet && quietProgress == 0 {
			log.Printf("Read %.2f million features\r", float64(*sst.ProgresSeq)/1000000.0)
		}
	}

	(*(sst.ProgresSeq))++
	(*(sst.LayerSeq))++

	return 1
}

//DeserializeFeature 反序列化要素
func DeserializeFeature(geoms *os.File, geomposIn *int64, metabase []byte, mataOff []int64, z, tx, ty uint, initialX, initialY []uint) SerialFeature {
	var sf SerialFeature
	bt := make([]byte, 1)
	n, err := geoms.Read(bt)
	if err != nil {
		log.Error(err)
	}
	log.Printf("read %d byte\n", n)
	sf.T = int8(bt[0])
	if sf.T < 0 {
		return sf
	}
	rd := bufio.NewReader(geoms)
	n64, readerr := binary.ReadVarint(rd)
	if readerr != nil {
		return sf
	}
	log.Printf("read %d byte\n", n64)
	return sf
}

//SerializeFeatureGeom 序列化featuregeometry
func SerializeFeatureGeom(file *os.File, sf *SerialFeature, geompos *int64, wx, wy int64, includeMinzoom bool) error {
	n, err := file.Write([]byte{byte(sf.T)})
	atomic.AddInt64(geompos, int64(n))
	var layer int64
	layer |= int64(sf.Layer) << 6
	var bseq int64
	if sf.Seq != 0 {
		bseq = 1
	}
	layer |= bseq << 5
	var bindex int64
	if sf.Index != 0 {
		bindex = 1
	}
	layer |= bindex << 4
	var bextent int64
	if sf.Extent != 0 {
		bextent = 1
	}
	layer |= bextent << 3
	var hasid int64
	if sf.HasID {
		hasid = 1
	}
	layer |= hasid << 2
	var hasminzoom int64
	if sf.HasMinzoom {
		hasminzoom = 1
	}
	layer |= hasminzoom << 1
	var hasmaxzoom int64
	if sf.HasMaxzoom {
		hasmaxzoom = 1
	}
	layer |= hasmaxzoom << 0

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, layer)
	n, err = file.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	atomic.AddInt64(geompos, int64(n))

	if sf.Seq != 0 {
		binary.PutVarint(buf, sf.Seq)
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}
	if sf.HasMinzoom {
		binary.PutVarint(buf, int64(sf.Minzoom))
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}
	if sf.HasMaxzoom {
		binary.PutVarint(buf, int64(sf.Maxzoom))
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}
	if sf.HasID {
		binary.PutVarint(buf, sf.ID)
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}
	binary.PutVarint(buf, int64(sf.Segment))
	n, err = file.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	atomic.AddInt64(geompos, int64(n))
	//write geomtry
	WriteGeom(sf.geometry, geompos, file, wx, wy)
	if sf.Index != 0 {
		binary.PutVarint(buf, int64(sf.Index))
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}
	if sf.Extent != 0 {
		binary.PutVarint(buf, sf.Extent)
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}

	binary.PutVarint(buf, sf.Metapos)
	n, err = file.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	atomic.AddInt64(geompos, int64(n))

	if sf.Metapos < 0 {
		binary.PutVarint(buf, int64(len(sf.Keys)))
		n, err = file.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))

		for i := range sf.Keys {
			binary.PutVarint(buf, sf.Keys[i])
			n, err = file.Write(buf)
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(geompos, int64(n))

			binary.PutVarint(buf, sf.Values[i])
			n, err = file.Write(buf)
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(geompos, int64(n))
		}
	}

	if includeMinzoom {
		n, err := file.Write([]byte{byte(sf.FeatureMinzoom)})
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))
	}
	return nil
}

//WriteGeom 写GEOM
func WriteGeom(dv DrawVec, fpos *int64, out *os.File, wx, wy int64) {
	buf := make([]byte, binary.MaxVarintLen64)
	for i := range dv {
		if dv[i].Opt == vtMoveto || dv[i].Opt == vtLineto {
			n, err := out.Write([]byte{byte(dv[i].Opt)})
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(fpos, int64(n))
			binary.PutVarint(buf, dv[i].X-wx)
			n, err = out.Write(buf)
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(fpos, int64(n))
			binary.PutVarint(buf, dv[i].Y-wy)
			n, err = out.Write(buf)
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(fpos, int64(n))
			wx = dv[i].X
			wy = dv[i].Y
		} else {
			n, err := out.Write([]byte{byte(dv[i].Opt)})
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(fpos, int64(n))
		}
	}
}

//ScaleGeometry xx
func ScaleGeometry(sst *SerializationState, bbox []int64, geom DrawVec) int {
	var offset, prev int64
	hasPrev := false
	var t int64 = 1 << geometryScale
	scale := 1.0 / float64(t)

	fmt.Println(offset, prev, hasPrev, t, scale)
	for i, v := range geom {
		fmt.Println(v)
		if v.Opt == vtMoveto || v.Opt == vtLineto {
			x, y := geom[i].X, geom[i].Y
			if additional[aDetectWraparound] != 0 {
				x += offset
				if hasPrev {
					if x-prev > (1 << 32) {
						offset -= 1 << 32
						x -= 1 << 32
					} else if prev-x > (1 << 32) {
						offset += 1 << 32
						x += 1 << 32
					}
				}
				hasPrev = true
				prev = x
			}
			//get geom box
			if x < bbox[0] {
				bbox[0] = x
			}
			if y < bbox[1] {
				bbox[1] = y
			}
			if x > bbox[2] {
				bbox[2] = x
			}
			if y > bbox[3] {
				bbox[3] = y
			}
			if *(sst.Initialized) == 0 {
				if x < 0 || x >= (1<<32) || y < 0 || y >= (1<<32) {
					*(sst.InitialX) = 1 << 31
					*(sst.InitialY) = 1 << 31
				} else {
					*(sst.InitialX) = uint(((x + CoordOffset) >> geometryScale) << geometryScale)
					*(sst.InitialY) = uint(((y + CoordOffset) >> geometryScale) << geometryScale)
				}
				*(sst.Initialized) = 1
			}
			if additional[aGridLowZooms] != 0 {
				geom[i].X = int64(math.Round(float64(x) * scale))
				geom[i].X = int64(math.Round(float64(y) * scale))
			} else {
				geom[i].X = ShiftRight(x)
				geom[i].Y = ShiftRight(y)
			}
		}
	}
	return len(geom)
}

//FixPolygon xxx
func FixPolygon(geom DrawVec) DrawVec {
	out := DrawVec{}
	return out
}

//GetArea 计算面积
func GetArea(geom DrawVec, i, j int) float64 {
	var area float64
	for k := i; k < j; k++ {
		area += float64(geom[k].X) * float64(geom[i+((k-i+1)%(j-i))].Y)
		area -= float64(geom[k].Y) * float64(geom[i+((k-i+1)%(j-i))].X)
	}
	area /= 2
	return area
}

//CoerceValue xx
func CoerceValue(key string, vt *int, val *string, attrTypes map[string]int) {
	if atype, ok := attrTypes[key]; ok {
		switch atype {
		case mvtString:
			*vt = mvtString
		case mvtFloat:
			*vt = mvtDouble
		case mvtInt:
			*vt = mvtDouble
			if len(*val) == 0 {
				*val = "0"
			}
			fv, _ := strconv.ParseFloat(*val, 64)
			iv := int64(math.Round(fv))
			*val = strconv.FormatInt(iv, 10)
		case mvtBool:
			var fval float64 = -1
			if *vt == mvtDouble {
				fval, _ = strconv.ParseFloat(*val, 64)
			}
			if *val == "false" || *val == "0" || *val == "null" ||
				len(*val) == 0 || fval == 0 {
				*vt = mvtBool
				*val = "false"
			} else {
				*vt = mvtBool
				*val = "true"
			}
		default:
			log.Fatalf("Can't happen: attribute type %d\n", atype)
		}
	}
}
