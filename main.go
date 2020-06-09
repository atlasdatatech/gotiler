package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/shirou/gopsutil/mem"
	"github.com/tysonmote/gommap"

	log "github.com/sirupsen/logrus"
)

const (
	//MaxUint 最大无符号整数
	MaxUint = ^uint(0)
	//MinUint 最小无符号整数
	MinUint = 0
	//MaxInt 最大有符号整数
	MaxInt = int(MaxUint >> 1)
	//MinInt 最小有符号整数
	MinInt = ^MaxInt
)

const (
	lowDetail  = 12
	fullDetail = -1
	minDetail  = 7
)

const (
	//MaxZoom 最大级别
	MaxZoom = 24
)

var quiet = false
var quietProgress = 0

var progressInterval float64
var lastProgress float64 //std::atomic<double> last_progress(0);
var geometryScale uint
var simplification float64
var maxTileSize = 500000
var maxTileFeatures = 200000
var clusterDistance = 0
var justx = -1
var justy = -1

var attributeForID = ""

var prevent [256]int
var additional [256]int

type source struct {
	Layer       string
	File        string
	Description string
	Format      string
}

var cpus int
var tempFiles int

//获取系统能打开的最大文件数,写死1024
var maxFiles = 1024

var diskfree int64

//Index index
type Index struct {
	Start   int64
	End     int64
	Ix      uint64
	Segment int
	T       int8
	Seq     int64
}

//Clipbox xxx
type Clipbox struct {
	lon1 float32
	lat1 float32
	lon2 float32
	lat2 float32

	minx int64
	miny int64
	maxx int64
	maxy int64
}

var args []string

var clipboxes []Clipbox

//MergeList xxx
type MergeList struct {
	Start int64
	End   int64
	Next  *MergeList
}

//DropState xx
type DropState struct {
	Gap       float64
	PrevIndex uint64
	Interval  float64
	Scale     float64
	Seq       float64
	Included  int64
	X         uint
	Y         uint
}

func atoiRequire(s, what string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("%s: %s must be a number (got %s)", strings.Join(args, " "), what, s)
	}
	i64, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return int(i64), nil
}

func atofRequire(s, what string) (float32, error) {
	if s == "" {
		return 0, fmt.Errorf("%s: %s must be a number (got %s)", strings.Join(args, " "), what, s)
	}
	f64, err := strconv.ParseFloat(s, 32)

	if err != nil {
		return 0, err
	}
	return float32(f64), nil
}

func atollRequire(s, what string) (float64, error) {
	if s == "" {
		return 0, fmt.Errorf("%s: %s must be a number (got %s)", strings.Join(args, " "), what, s)
	}
	f64, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return f64, nil
}

func initCups() {
	var err error
	maxThreads := os.Getenv("MAX_THREADS")
	if maxThreads != "" {
		cpus, err = atoiRequire(maxThreads, "TIPPECANOE_MAX_THREADS")
		if err != nil {
			log.Error(err)
		}
	} else {
		cpus = runtime.NumCPU()
	}
	if cpus < 1 {
		cpus = 1
	}

	// Guard against short struct index.segment
	if cpus > 32767 {
		cpus = 32767
	}

	// Round down to a power of 2
	cpus = 1 << (uint)(math.Log(float64(cpus))/math.Log(2))

	//Create maxnum files array
	fds := make([]*os.File, maxFiles)
	i := 0
	for ; i < maxFiles; i++ {
		fds[i], err = os.Open(os.DevNull)
		if err != nil {
			log.Fatal(err)
		}
	}

	for j := 0; j < i; j++ {
		err := fds[j].Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	// MAX_FILES = i * 3 / 4
	tempFiles = int((maxFiles - 10) / 2)
	if tempFiles > cpus*4 {
		tempFiles = cpus * 4
	}

	log.Println("CPUs:", cpus)
	log.Println("MaxFiles:", maxFiles)
	log.Println("TemFiles:", tempFiles)
}

//DiskStatus 磁盘空间状态
type DiskStatus struct {
	All  int64
	Used int64
	Free int64
}

//DiskUsage 磁盘使用情况
func DiskUsage(path string) (disk DiskStatus) {
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")
	lpFreeBytesAvailable := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)
	c.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))
	disk.All = lpTotalNumberOfBytes
	disk.Used = lpTotalNumberOfBytes - lpTotalNumberOfFreeBytes
	disk.Free = lpFreeBytesAvailable
	return
}

//CheckDisk 检查磁盘空间
func CheckDisk(readers []Reader) {
	var used int64
	for _, r := range readers {
		used += r.Metapos + 2*r.Geompos + 2*r.Indexpos + r.PoolMemFile.Len + r.TreeMemFile.Len
	}
	if used > int64(float64(diskfree)*.9) {
		fmt.Fprintf(os.Stderr, "You will probably run out of disk space.\n%d bytes used or committed, of %d originally available\n", used, diskfree)
		log.Warningf("You will probably run out of disk space.\n%d bytes used or committed, of %d originally available\n", used, diskfree)
	}
}

//IntToBytes 整形转换成字节
func IntToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//BytesToInt 字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int(x)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func main() {

	// Below is an example of using our PrintMemUsage() function
	// Print our starting memory usage (should be around 0mb)
	PrintMemUsage()

	var overall [][]int
	for i := 0; i < 4; i++ {

		// Allocate memory using make() and append to overall (so it doesn't get
		// garbage collected). This is to create an ever increasing memory usage
		// which we can track. We're just using []int as an example.
		a := make([]int, 0, 999999)
		overall = append(overall, a)

		// Print our memory usage at each interval
		PrintMemUsage()
		// time.Sleep(time.Second * 5)
	}

	// Clear our memory and print usage, unless the GC has run 'Alloc' will remain the same
	overall = nil
	PrintMemUsage()
	// Force GC to clear up, should see a memory drop
	runtime.GC()
	PrintMemUsage()

	xs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	xs = xs[:len(xs)-1]
	fmt.Println(xs)
	for i := len(xs) - 1; i >= 0; i-- {
		// v := xs[i]
		// if i%2 == 0 {
		xs = append(xs[:i], xs[i+1:]...)
		// fmt.Printf("after delete xs[%d] =%d elem, ", i, v)
		// }
		fmt.Println(xs)
	}
	// fmt.Println(append(xs[:0], xs[1:]...))

	fmt.Println(strings.TrimPrefix("000123", "0"))
	fmt.Println(strings.TrimLeft("000123", "0"))

	f, err := os.Create("d:/a.txt")
	if err != nil {
		fmt.Println(err)
	} else {
		defer f.Close()
	}
	ff, err := os.Create("d:/a/b.txt")
	if err != nil {
		fmt.Println(err)
	} else {
		defer ff.Close()
	}
	fmt.Println(MaxUint, MinUint, MaxInt, MinInt)
	if MinInt == -MaxInt-1 {
		fmt.Println("yes")
	}

	var x int64 = 1
	var y int64 = -1
	bb := bytes.NewBuffer([]byte{})
	err = binary.Write(bb, binary.BigEndian, x)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(bb, binary.BigEndian, y)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("byte len: ", bb.Len(), ", bytes: ", bb.Bytes())
	buf := make([]byte, binary.MaxVarintLen64)
	bufsize := binary.PutVarint(buf, x)
	fmt.Println("buf size: ", bufsize, ", buf: ", buf[:bufsize])
	bufsize2 := binary.PutVarint(buf[bufsize:], y)
	fmt.Println("buf size: ", bufsize2, ", buf: ", buf[bufsize:bufsize+bufsize2])
	bufsize3 := binary.PutUvarint(buf, 1)
	fmt.Println("buf size: ", bufsize3, ", buf: ", buf[bufsize+bufsize2:bufsize+bufsize2+bufsize3])

	args = os.Args
	initCups()

	disk := DiskUsage("C:/Users/Administrator")
	fmt.Println(disk)

	fmt.Println(1 << 62)
	var ll int64
	ll = 1 << 62
	fmt.Println(ll)

	splits := 100
	splitbits := uint(math.Log(float64(splits)) / math.Log(2))
	splits = 1 << splitbits
	fmt.Println(splits)
	// basezoom := 10
	// for i := 0; i <= basezoom; i++ {
	// 	if i < basezoom {
	// 		interval := math.Exp(math.Log(2.5) * float64((basezoom - i)))
	// 		fmt.Println(i, interval)
	// 	}
	// 	s := 1 << uint8(64-2*(i+8))
	// 	scale := float64(s)
	// 	fmt.Println("scale:", scale)
	// }
	// var optind int
	// var i int
	// var optarg, name, layername, out_mbtiles, out_dir string
	outdir := "."
	outmbtiles := "out.mbtiles"
	force := true
	if force {
		err := os.Remove(outmbtiles)
		if err != nil {
			log.Fatal(err)
		}
		dir := filepath.Dir(outmbtiles)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}
	outdb, err := mbtilesOpen(outmbtiles)
	if err != nil {
		log.Fatal(err)
	}
	maxzoom := 14
	minzoom := 0
	basezoom := -1
	basezoomMarkerWidth := 1.0
	// force := 0
	// forcetable := 0
	droprate := 2.5
	gamma := 0.0
	buffer := 5
	const tmpDir = "/tmp"
	// const attribution = ""
	var sources []source
	// const prefilter = ""
	// const postfilter = ""
	var guessMaxzoom = false

	// var exclude, include []string
	// attribute_types := make(map[string]int)
	// attribute_accum := make(map[string]attribute_op)
	// attribute_descriptions := make(map[string]string)
	// var exclude_all = 0
	// var read_parallel = 0
	// var files_open_at_start int
	// json_object * filter = NULL
	src := source{Layer: "PointLayer", File: "./banks.csv", Description: "main process testing", Format: ".csv"}
	sources = append(sources, src)
	name := "outputname"

	ReadInput(sources, name, maxzoom, minzoom, basezoom, basezoomMarkerWidth, outdb, outdir, tmpDir, buffer, guessMaxzoom, droprate, gamma)

	file, err := os.Open("tt.json")
	if err != nil {
		log.Fatal(err)
	}

	mmap, err := gommap.Map(file.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	if err != nil {
		log.Fatal(err)
	}
	end := bytes.Index(mmap, []byte("\n"))
	fmt.Println(string([]byte(mmap[:end])))

	sp := StringPool{1, 2, 3}
	type SliceMock struct {
		addr uintptr
		len  int
		cap  int
	}
	l := unsafe.Sizeof(sp)
	fmt.Println("struct size: ", l)

	spb := &SliceMock{
		addr: uintptr(unsafe.Pointer(&sp)),
		cap:  int(l),
		len:  int(l),
	}

	pdata := (*[]byte)(unsafe.Pointer(spb))
	fmt.Println("[]byte is : ", *pdata)
	nspm := *(**StringPool)(unsafe.Pointer(pdata))
	fmt.Println("new stringpool of mockslice:", *nspm)

	afile, err := ioutil.TempFile(tmpDir, "a.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(afile.Name())
	bfile, err := ioutil.TempFile(tmpDir, "b.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(bfile.Name())
	cfile, err := ioutil.TempFile(tmpDir, "c.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfile.Name())
	type SP struct {
		L int
		R int
		O int
	}
	n := 10000
	sps := []Index{}
	for i := 0; i < n; i++ {
		sps = append(sps, Index{})
	}
	fmt.Printf("size of sps slice : %d", len(sps))
	start := time.Now()
	for i := range sps {
		size := unsafe.Sizeof(sps[i])
		bp := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&sps[i])))[:size]
		afile.Write(bp)
	}
	fmt.Printf("a, serailiez %d stringpools take %0.2f s\n", n, time.Since(start).Seconds())

	start = time.Now()
	for i := range sps {
		size := unsafe.Sizeof(sps[i])
		spb := &SliceMock{
			addr: uintptr(unsafe.Pointer(&sps[i])),
			cap:  int(size),
			len:  int(size),
		}
		pdata := *(*[]byte)(unsafe.Pointer(spb))
		bfile.Write(pdata)
	}
	fmt.Printf("b, serailiez %d stringpools take %0.2f s\n", n, time.Since(start).Seconds())
	//gob
	// var bbuf bytes.Buffer
	// gob.NewEncoder(&bbuf)
	start = time.Now()
	enc := gob.NewEncoder(cfile)
	for i := range sps {
		enc.Encode(sps[i])
	}
	fmt.Printf("c, serailiez %d stringpools take %0.2f s\n", n, time.Since(start).Seconds())

	// err = afile.Close()
	// if err != nil {
	// 	log.Error(err)
	// }
	// err = bfile.Close()
	// if err != nil {
	// 	log.Error(err)
	// }
	// err = cfile.Close()
	// if err != nil {
	// 	log.Error(err)
	// }

	dsp := Index{}
	i := 0
	splen := unsafe.Sizeof(dsp)
	spbuf := make([]byte, splen)
	afile.Seek(0, 0)
	for {
		n, err = afile.Read(spbuf)
		i++
		if err == nil {
			nsp := *(*Index)(unsafe.Pointer(&spbuf[0]))
			fmt.Println(i, n, nsp)
		} else if err == io.EOF {
			break
		} else {
			fmt.Println(i, err)
		}
	}
	bfile.Seek(0, 0)
	for {
		n, err = bfile.Read(spbuf)
		i++
		if err == nil {
			nsp := *(*Index)(unsafe.Pointer(&spbuf[0]))
			fmt.Println(i, n, nsp)
		} else if err == io.EOF {
			break
		} else {
			fmt.Println(i, err)
		}
	}
	cfile.Seek(0, 0)
	dec := gob.NewDecoder(cfile)
	for {
		err = dec.Decode(&dsp)
		i++
		if err == nil {
			fmt.Println(i, dsp)
		} else if err == io.EOF {
			break
		} else {
			fmt.Println(i, err)
		}
	}

	bp := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&sp)))[:l]
	fmt.Println("[]byte is : ", bp)
	nsp := *(*StringPool)(unsafe.Pointer(&bp[0]))
	fmt.Println("new stringpool:", nsp)

	file, err = os.OpenFile("test.txt", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Error(err)
		return
	}
	mf := MemFileOpen(file)
	if mf == nil {
		return
	}
	Len := MemFileWrite(mf, bp)
	fmt.Println(Len)
	if Len != int(l) {
		log.Errorf("MemFileWrite error ,the return %d error", Len)
		// return
	}
	f, err = os.Open("test.txt")
	if err != nil {
		log.Error(err)
		return
	}
	obuf := make([]byte, Len)
	f.Read(obuf)
	nnsp := *((*StringPool)(unsafe.Pointer(&obuf[0])))
	fmt.Println("nnsp:", nnsp)
}

//ReadInput 处理
func ReadInput(sources []source, name string, maxzoom, minzoom, basezoom int, basezoomMarkerWidth float64, outdb *sql.DB, outdir string, tmpDir string, buffer int, guessMaxzoom bool, droprate, gamma float64) int {

	//in args
	prefilter := ""
	postfilter := ""
	useGamma := false
	excludeAll := false
	exclude := []string{}
	include := []string{}
	attributeTypes := make(map[string]int)

	readers := make([]Reader, cpus)
	for i := range readers {
		r := &readers[i]
		var err error
		r.Metafile, err = ioutil.TempFile(tmpDir, "meta.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(r.Metafile.Name())
		r.Geomfile, err = ioutil.TempFile(tmpDir, "geom.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(r.Geomfile.Name())
		r.Indexfile, err = ioutil.TempFile(tmpDir, "index.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(r.Indexfile.Name())

		r.Poolfile, err = ioutil.TempFile(tmpDir, "pool.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(r.Poolfile.Name())

		r.Treefile, err = ioutil.TempFile(tmpDir, "tree.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(r.Treefile.Name())

		r.PoolMemFile = MemFileOpen(r.Poolfile)
		r.TreeMemFile = MemFileOpen(r.Treefile)

		r.Metapos = 0
		r.Geompos = 0
		r.Indexpos = 0

		//to distingush a null value
		sp := StringPool{}
		size := unsafe.Sizeof(sp)
		bp := (*(*[1<<31 - 1]byte)(unsafe.Pointer(&sp)))[:size]
		if MemFileWrite(r.TreeMemFile, bp) != int(size) {
			log.Error("MemFileWrite error")
		}

		buf := make([]byte, binary.MaxVarintLen64)
		s := binary.PutVarint(buf, 0)
		n, err := r.Metafile.Write(buf[:s])
		if err != nil {
			log.Fatalf("meta: Write to temporary file failed, error: %s", err)
		}
		atomic.AddInt64(&r.Metapos, int64(n))

		r.FileBBox[0] = math.MaxInt64
		r.FileBBox[1] = math.MaxInt64
		r.FileBBox[2] = 0
		r.FileBBox[3] = 0
	}
	//the tmpdir
	fname := readers[0].Geomfile.Name()
	if len(fname) > 1 && fname[1] != ':' {
		fname = filepath.VolumeName(os.Args[0])
	}
	disk := DiskUsage(fname)
	fmt.Println(disk)
	diskfree = disk.Free

	var progressSeq int64

	initialized := make([]int, 2*cpus)
	initialX := make([]uint, 2*cpus)
	initialY := make([]uint, 2*cpus)

	fmt.Println(initialized)
	fmt.Println(initialX, initialY)
	fmt.Println(progressSeq)

	for i, s := range sources {
		if len(s.Layer) == 0 {
			fname := ""
			if len(s.File) == 0 {
				fname = name
			} else {
				fname = s.File
			}
			base := filepath.Base(fname)
			ext := filepath.Ext(fname)
			layername := strings.TrimSuffix(base, ext)
			if len(layername) == 0 || !utf8.Valid([]byte(layername)) {
				layername = "unknow" + strconv.Itoa(i)
			}
			sources[i].Layer = layername
			if !quiet {
				fmt.Fprintf(os.Stderr, `For layer %d, using name "%s" \n`, i, sources[i].Layer)
			}
		}

	}

	layermap := make(map[string]LayerEntry)
	for i, s := range sources {
		e := LayerEntry{ID: i}
		e.Description = s.Description
		layermap[s.Layer] = e
	}
	layermaps := make([]map[string]LayerEntry, cpus)
	for i := range layermaps {
		layermaps[i] = layermap
	}

	var overallOffset int64
	var distSum float64
	var distCount int

	fmt.Println(overallOffset, distSum, distCount)

	for i, s := range sources {
		fmt.Println(i)
		reading := ""
		var file *os.File
		if len(s.File) == 0 {
			reading = "standard input"
		} else {
			reading = s.File
			var err error
			if file, err = os.Open(s.File); err != nil {
				log.Error(err)
				continue
			}
			defer file.Close()
		}
		fmt.Println(reading)
		fmt.Println(file.Name())
		layer, ok := layermap[s.Layer]
		if !ok {
			log.Fatalf("Internal error: couldn't find layer %s", s.Layer)
		}
		fmt.Println(layer)
		//处理geobuf格式数据
		if s.Format == "geobuf" || strings.ToLower(filepath.Ext(s.File)) == ".geobuf" {
			continue
		}
		//处理csv格式的数据
		if s.Format == "csv" || strings.ToLower(filepath.Ext(s.File)) == ".csv" {
			layerSeqs := make([]int64, cpus)
			distSums := make([]float64, cpus)
			distCounts := make([]int64, cpus)
			sst := make([]SerializationState, cpus)
			for i := 0; i < cpus; i++ {
				layerSeqs[i] = overallOffset
				distSums[i] = 0
				distCounts[i] = 0
				sst[i].FName = reading
				sst[i].Line = 0
				sst[i].LayerSeq = &layerSeqs[i]
				sst[i].Readers = readers
				sst[i].Segment = i
				sst[i].InitialX = &initialX[i]
				sst[i].InitialY = &initialY[i]
				sst[i].Initialized = &initialized[i]
				sst[i].DistSum = &distSums[i]
				sst[i].DistCount = &distCounts[i]
				sst[i].WantDist = guessMaxzoom
				sst[i].Maxzoom = maxzoom
				sst[i].Filters = len(prefilter) != 0 || len(postfilter) != 0
				sst[i].UsesGamma = useGamma
				sst[i].LayerMap = layermaps[i]
				sst[i].Exclude = exclude
				sst[i].Include = include
				sst[i].ExcludeAll = excludeAll
				sst[i].Basezoom = basezoom
				sst[i].AttributeTypes = attributeTypes
			}

			ParseGeoCSV(sst, file, layer.ID, s.Layer)
			overallOffset = layerSeqs[0]
			CheckDisk(readers)
			continue
		}
		//处理geojson格式数据，判断能否并行化处理，否 则顺序处理
	}

	for i := range readers {
		err := readers[i].Metafile.Close()
		if err != nil {
			log.Fatal("close tmp meta file error:", err)
		}
		err = readers[i].Geomfile.Close()
		if err != nil {
			log.Fatal("close tmp geom file error:", err)
		}
		err = readers[i].Indexfile.Close()
		if err != nil {
			log.Fatal("close tmp index file error:", err)
		}
		err = MemFileClose(readers[i].TreeMemFile)
		if err != nil {
			log.Fatal("close tmp tree memfile error:", err)
		}

		readers[i].Geomst, err = readers[i].Geomfile.Stat()
		if err != nil {
			log.Fatal("read tmp geom fileinfo stat error:", err)
		}
		readers[i].Metast, err = readers[i].Metafile.Stat()
		if err != nil {
			log.Fatal("read tmp meta fileinfo stat error:", err)
		}
	}

	poolOff := make([]int64, 2*cpus)
	metaOff := make([]int64, 2*cpus)
	poolfile, err := ioutil.TempFile(tmpDir, "pool.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(poolfile.Name())
	metafile, err := ioutil.TempFile(tmpDir, "meta.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(metafile.Name())

	var metapos, poolpos int64

	for i := range readers {
		if readers[i].Metapos > 0 {
			mmap, err := gommap.Map(readers[i].Metafile.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
			if err != nil {
				log.Fatal(err)
			}
			n, err := metafile.Write(mmap)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("metafile write %d bytes metadata", n)
			mmap.UnsafeUnmap()
			//其实可以直接将文件复制到新文件，如下
			// io.Copy(metafile, readers[i].Metafile)
		}
		metaOff[i] = metapos
		metapos += readers[i].Metapos
		//the cpp should close the fd
		if readers[i].PoolMemFile.Off > 0 {
			n, err := poolfile.Write(readers[i].PoolMemFile.Map)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("reunify string pool %d bytes ", n)
		}
		poolOff[i] = poolpos
		poolpos += readers[i].PoolMemFile.Off
		err = MemFileClose(readers[i].PoolMemFile)
		if err != nil {
			log.Fatal("close pool memfile error:", err)
		}

	}

	//needed to mmap following, should close?
	err = poolfile.Close()
	if err != nil {
		log.Fatal("close tmp pool file error:", err)
	}
	err = metafile.Close()
	if err != nil {
		log.Fatal("close tmp meta file error:", err)
	}

	meta, err := gommap.Map(metafile.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	if err != nil {
		log.Fatal(err)
	}
	// madvise(meta,RANDOM)
	var stringpool gommap.MMap
	if poolpos > 0 {
		var err error
		stringpool, err = gommap.Map(poolfile.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
		if err != nil {
			log.Fatal(err)
		}
		// madvise(poolfile, RANDOM)
	}

	fmt.Println(meta)
	fmt.Println(stringpool)

	indexfile, err := ioutil.TempFile(tmpDir, "index.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(indexfile.Name())
	geomfile, err := ioutil.TempFile(tmpDir, "geom.*")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(geomfile)

	//choose frist zoom
	filebbox := [4]int64{math.MaxInt64, math.MaxInt64, 0, 0}
	iz, ix, iy := ChooseFirstZoom(filebbox[:], readers, minzoom, buffer)

	if justx >= 0 {
		iz = minzoom
		ix = uint(justx)
		iy = uint(justy)
	}

	fmt.Println(iz, ix, iy)
	var geompos int64
	buf64 := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf64, int64(iz))
	n, err := geomfile.Write(buf64)
	if err != nil {
		log.Fatal(err)
	}
	atomic.AddInt64(&geompos, int64(n))
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, ix)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(buf, binary.LittleEndian, iy)
	if err != nil {
		log.Fatal(err)
	}
	n, err = geomfile.Write(buf.Bytes())
	atomic.AddInt64(&geompos, int64(n)) //x & y len

	//radix
	radix(readers, geomfile, indexfile, tmpDir, &geompos, maxzoom, basezoom, droprate, gamma)
	var b2 int8 = -2
	n, err = geomfile.Write([]byte{byte(b2)})
	if err != nil {
		log.Fatal(err)
	}
	atomic.AddInt64(&geompos, int64(n))

	err = geomfile.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = indexfile.Close()
	if err != nil {
		log.Fatal(err)
	}

	indexst, err := indexfile.Stat()
	if err != nil {
		log.Fatal(err)
	}
	indexpos := indexst.Size()
	progressSeq = indexpos / int64(unsafe.Sizeof(Index{}))
	lastProgress = 0
	if !quiet {
		fmt.Printf("%d features, %d bytes of geomtry, %d bytes of separate metadata,%d bytes of string pool\n", progressSeq, geompos, metapos, poolpos)
	}

	if indexpos == 0 {
		err := mbtilesClose(outdb)
		if err != nil {
			log.Fatal(err)
		}
		log.Fatalf("did not read any valid geometries\n")
	}

	//完成数据读取，开始切片
	indexmap, err := gommap.Map(indexfile.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	if err != nil {
		log.Fatal(err)
	}
	//madvise
	// madvise(map, indexpos, MADV_SEQUENTIAL)
	// madvise(map, indexpos, MADV_WILLNEED)
	idxSize := int64(unsafe.Sizeof(Index{}))
	indices := indexpos / int64(idxSize)
	log.Printf("index count: %d\n", indices)
	fixDropping := false

	if guessMaxzoom {
		var sum float64
		var progress int64 = -1
		var count, ip int64
		for ip = idxSize; ip < indexpos; ip += idxSize {
			idx1 := (*Index)(unsafe.Pointer(&indexmap[ip]))
			idx2 := (*Index)(unsafe.Pointer(&indexmap[ip-idxSize]))
			if idx1.Ix != idx2.Ix {
				count++
				sum += math.Log(float64(idx1.Ix - idx2.Ix))
			}
			nprogress := 100 * ip / indexpos
			if nprogress != progress {
				progress = nprogress
				if !quiet && quietProgress == 0 && progressTime() {
					log.Errorf("Maxzoom: %d %%", progress)
				}
			}

			if count == 0 && distCount == 0 {
				log.Fatalf("Can't guess maxzoom (-zg) without at least two distinct feature locations\n")
				// if outdb!=nil{
				// // mbtiles_close(outdb, pgm);
				// }
			}

			if count > 0 {

				avg := math.Exp(sum / float64(count))
				distFt := math.Sqrt(avg) / 33
				want := distFt / 8
				maxzoom = int(math.Ceil(math.Log(360/(want*0.00000274))/math.Log(2) - fullDetail))

				if maxzoom < 0 {
					maxzoom = 0
				}
				if maxzoom > 32-fullDetail {
					maxzoom = 32 - fullDetail
				}
				if maxzoom > 33-lowDetail {
					maxzoom = 33 - lowDetail
				}

				if !quiet {
					log.Printf("Choosing a maxzoom of -z%d for features about %d feet (%d meters) apart\n", maxzoom, int(math.Ceil(distFt)), int(math.Ceil(distFt/3.28084)))
				}

				changed := false

				for maxzoom < 32-fullDetail && maxzoom < 33-lowDetail && clusterDistance > 0 {

					zoomMingap := (1 << (32 - uint(maxzoom)) / 256 * clusterDistance) * (1 << (32 - uint(maxzoom)) / 256 * clusterDistance)
					if avg > float64(zoomMingap) {
						break
					}
					maxzoom++
					changed = true
				}
				if changed {
					log.Printf("Choosing a maxzoom of -z%d to keep most features distinct with cluster distance %d\n", maxzoom, clusterDistance)
				}

			}

			if distCount != 0 {
				want2 := math.Exp(distSum/float64(distCount)) / 8
				mz := int(math.Ceil(math.Log(360/(0.00000274*want2))/math.Log(2) - fullDetail))

				if mz < 0 {
					mz = 0
				}
				if mz > 32-fullDetail {
					mz = 32 - fullDetail
				}
				if mz > 33-lowDetail {
					mz = 33 - lowDetail
				}

				if mz > maxzoom || count <= 0 {
					if !quiet {
						log.Printf("Choosing a maxzoom of -z%d for resolution of about %d feet (%d meters) within features\n", mz, int(math.Exp(distSum/float64(distCount))), int(math.Exp(distSum/float64(distCount))/3.28084))
					}
					maxzoom = mz
				}

			}

			if maxzoom < minzoom {
				log.Printf("Can't use %d for maxzoom because minzoom is %d\n", maxzoom, minzoom)
				maxzoom = minzoom
			}
			fixDropping = true
			if basezoom == -1 {
				basezoom = maxzoom
			}
		}

	}

	if basezoom < 0 || droprate < 0 {

		type Tile struct {
			x         uint64
			y         uint64
			count     int64
			fullcount int64
			gap       float64
			preindex  uint64
		}

		var tile, max [MaxZoom + 1]Tile

		var progress int64 = -1
		var ip int64
		for ip = 0; ip < indexpos; ip += idxSize {
			idx := (*Index)(unsafe.Pointer(&indexmap[ip]))
			xx, yy := DecodeQuadkey(idx.Ix)

			nprogress := 100 * ip / indexpos
			if nprogress != progress {
				if !quiet && quietProgress == 0 && progressTime() {
					log.Printf("Base zoom/drop rate: %d %% \r", progress)
				}
			}
			z := 0
			for z = 0; z <= MaxZoom; z++ {
				var xxx, yyy uint64
				if z != 0 {
					xxx = xx >> (32 - z)
					yyy = yy >> (32 - z)
				}

				scale := (1 << (64 - 2*(z+8)))
				if tile[z].x != xxx || tile[z].y != yyy {
					if tile[z].count > max[z].count {
						max[z] = tile[z]
					}
					tile[z].x = xxx
					tile[z].y = yyy
				}
				tile[z].fullcount++

				if ManageGap(idx.Ix, &tile[z].preindex, float64(scale), gamma, &tile[z].gap) != 0 {
					continue
				}
				tile[z].count++
			}
		}

		z := 0
		for z = MaxZoom; z >= 0; z-- {
			if tile[z].count > max[z].count {
				max[z] = tile[z]
			}
		}

		maxFeatures := 50000 / (basezoomMarkerWidth * basezoomMarkerWidth)

		obasezoom := basezoom
		if basezoom < 0 {
			basezoom = MaxZoom
			for z = MaxZoom; z >= 0; z-- {
				if max[z].count < int64(maxFeatures) {
					basezoom = z
				}
			}
			if !quiet {
				log.Printf("Choosing a base zoom of -B%d to keep %d features in tile %d/%d/%d.\n", basezoom, max[basezoom].count, basezoom, max[basezoom].x, max[basezoom].y)
			}
		}

		if obasezoom < 0 && basezoom > maxzoom {
			log.Printf("Couldn't find a suitable base zoom. Working from the other direction.\n")
			if gamma == 0 {
				log.Printf("You might want to try -g1 to limit near-duplicates.\n")
			}
			if droprate < 0 {
				if maxzoom == 0 {
					droprate = 2.5
				} else {
					droprate = math.Exp(math.Log(float64(max[0].count)/float64(max[maxzoom].count)) / float64(maxzoom))
					if !quiet {
						log.Printf("Choosing a drop rate of -r%f to get from %d to %d in %d zooms\n", droprate, max[maxzoom].count, max[0].count, maxzoom)
					}
				}
			}

			basezoom = 0
			for z = 0; z <= maxzoom; z++ {
				zoomdiff := math.Log(float64(max[z].count)/maxFeatures) / math.Log(droprate)
				if zoomdiff+float64(z) > float64(basezoom) {
					basezoom = int(math.Ceil(zoomdiff + float64(z)))
				}
			}

			if !quiet {
				log.Printf("Choosing a base zoom of -B%d to keep %f features in tile %d/%d/%d.\n", basezoom, float64(max[maxzoom].count)*math.Exp(math.Log(droprate)*float64(maxzoom-basezoom)), maxzoom, max[maxzoom].x, max[maxzoom].y)
			}
		} else if droprate < 0 {
			droprate = 1
			for z = basezoom - 1; z >= 0; z-- {
				interval := math.Exp(math.Log(droprate) * float64(basezoom-z))
				if float64(max[z].count)/interval >= maxFeatures {
					interval = float64(max[z].count) / float64(maxFeatures)
					droprate = math.Exp(math.Log(interval) / float64(basezoom-z))
					interval = math.Exp(math.Log(droprate) * float64(basezoom-z))
					if !quiet {
						log.Printf("Choosing a drop rate of -r%f to keep %f features in tile %d/%d/%d.\n", droprate, float64(max[z].count)/interval, z, max[z].x, max[z].y)
					}
				}
			}
		}

		if gamma > 0 {

			effective := 0
			for z = 0; z < maxzoom; z++ {
				if max[z].count < max[z].fullcount {
					effective = z + 1
				}
			}

			if effective == 0 {
				if !quiet {
					log.Printf("With gamma, effective base zoom is 0, so no effective drop rate\n")
				}
			} else {
				interval0 := math.Exp(math.Log(droprate) * float64(basezoom-0))
				intervalEff := math.Exp(math.Log(droprate) * float64(basezoom-effective))
				if effective > basezoom {
					intervalEff = 1
				}
				scaled0 := float64(max[0].count) / interval0
				scaledEff := float64(max[effective].count) / intervalEff
				rateAt0 := scaled0 / float64(max[0].fullcount)
				rateAtEff := scaledEff / float64(max[effective].fullcount)

				effDrop := math.Exp(math.Log(rateAtEff/rateAt0) / float64(effective-0))
				if !quiet {
					log.Printf("With gamma, effective base zoom of %d, effective drop rate of %f\n", effective, effDrop)
				}
			}
		}
		fixDropping = true
	}

	if fixDropping {
		//计算出实际的basezoom和droprate后，调整minzoom

		geomap, err := gommap.Map(geomfile.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
		if err != nil {
			log.Fatal(err)
		}
		//madvise
		// madvise(geom, indexpos, MADV_SEQUENTIAL)
		// madvise(geom, indexpos, MADV_WILLNEED)

		ds := prepDropStates(maxzoom, basezoom, droprate)
		var ip int64
		for ip = 0; ip < indexpos; ip += idxSize {
			idx := (*Index)(unsafe.Pointer(&indexmap[ip]))
			if ip > idxSize {
				idx0 := (*Index)(unsafe.Pointer(&indexmap[ip-idxSize]))
				if idx.Start != idx0.End {
					log.Printf("Mismatched index at %d: %d vs %d\n", ip/idxSize, idx.Start, idx.End)
				}
				fminzoom := CalcFeatureMinzoom(idx, ds, maxzoom, gamma)
				geomap[idx.End-1] = []byte{byte(int8(fminzoom))}[0]
			}
			err := geomap.UnsafeUnmap()
			if err != nil {
				log.Fatal(err)
			}
		}

	}

	err = indexmap.UnsafeUnmap()
	if err != nil {
		log.Fatal(err)
	}
	err = indexfile.Close()
	if err != nil {
		log.Fatal(err)
	}

	files := make([]*os.File, tempFiles)
	fsizes := make([]int64, tempFiles)

	var midx, midy uint

	//切片
	TraverseZooms(files, fsizes, meta, stringpool, &midx, &midy, maxzoom, minzoom, outdb, outdir, tmpDir, buffer, gamma, fullDetail, lowDetail, minDetail, metaOff, poolOff, initialX, initialY, simplification, layermaps)

	return 0
}

//ChooseFirstZoom 计算第一个级别
func ChooseFirstZoom(filebox []int64, readers []Reader, minzoom, buffer int) (x int, y, z uint) {

	for _, r := range readers {
		if r.FileBBox[0] < filebox[0] {
			filebox[0] = r.FileBBox[0]
		}
		if r.FileBBox[1] < filebox[1] {
			filebox[1] = r.FileBBox[1]
		}
		if r.FileBBox[2] > filebox[2] {
			filebox[2] = r.FileBBox[2]
		}
		if r.FileBBox[3] > filebox[3] {
			filebox[3] = r.FileBBox[3]
		}
	}

	if filebox[0] < 0 {
		filebox[0] = 0
		filebox[2] = (1 << 32) - 1
	}
	if filebox[2] > (1<<32)-1 {
		filebox[0] = 0
		filebox[2] = (1 << 32) - 1
	}
	if filebox[1] < 0 {
		filebox[1] = 0
	}
	if filebox[3] > (1<<32)-1 {
		filebox[3] = (1 << 32) - 1
	}

	for z := minzoom; z >= 0; z-- {
		var shift int64 = 1 << uint(32-z)
		left := (filebox[0] - int64(buffer)*shift/256) / shift
		top := (filebox[1] - int64(buffer)*shift/256) / shift
		right := (filebox[2] - int64(buffer)*shift/256) / shift
		bottom := (filebox[3] - int64(buffer)*shift/256) / shift

		if left == right && top == bottom {
			return z, uint(left), uint(top)
		}
	}
	return
}

//Radix xx
func radix(readers []Reader, geomfile, indexfile *os.File, tmpdir string, geompos *int64, maxzoom, basezoom int, droprate, gamma float64) {
	nreaders := len(readers)
	v, _ := mem.VirtualMemory()
	// // almost every return value is a struct
	fmt.Printf("Total: %v, Free:%v, UsedPercent:%f%%\n", v.Total, v.Free, v.UsedPercent)
	// // convert to JSON. String() is also implemented
	fmt.Println(v)
	mem := int64(v.Total)
	if additional[aPreferRadixSort] != 0 {
		mem = 8192
	}
	availfiles := maxFiles - 2*nreaders - //each reader has a geom and a index
		4 - //pool,meta,mbtiles,mbtiles journal
		4 - //top-level geom and index output, both file and fd
		3 //stdin,stdout,stderr

	splits := availfiles / 4
	mem /= 2
	var geomTotal int64
	geomfiles := make([]*os.File, nreaders)
	indexfiles := make([]*os.File, nreaders)

	for i, r := range readers {

		geomfiles[i] = r.Geomfile
		indexfiles[i] = r.Indexfile

		gst, err := r.Geomfile.Stat()
		if err != nil {
			log.Fatal(err)
		}
		geomTotal += gst.Size()
	}

	ds := prepDropStates(maxzoom, basezoom, droprate)
	var progress int64
	var progressReported int64 = -1
	progressMax := geomTotal
	availfilesBefore := availfiles
	radix1(geomfiles, indexfiles, nreaders, 0, splits, mem, tmpdir, &availfiles, geomfile, indexfile, geompos, &progress, &progressMax, &progressReported, maxzoom, basezoom, droprate, gamma, ds)
	//inspect availfiles
	if availfiles-2*nreaders != availfilesBefore {
		//log.Fatal()
		fmt.Printf("Internal error: miscounted available file descriptors: %d vs %d\n", availfiles-2*nreaders, availfiles)
	}
}

//Radix1 xxx
func radix1(geomfilesIn, indexfilesIn []*os.File, inputs, prefix, splits int, mem int64, tmpdir string, availfiles *int, geomfile, indexfile *os.File, geomposOut, progress, progressMax, progressReported *int64, maxzoom, basezoom int, dorprate, gamma float64, ds []DropState) {
	//将子文件个数对齐到2的n次方,例如64/128/256
	splitbits := uint(math.Log(float64(splits)) / math.Log(2))
	splits = 1 << splitbits
	geomfiles := make([]*os.File, splits)
	indexfiles := make([]*os.File, splits)
	subGeompos := make([]int64, splits)
	ixsize := int(unsafe.Sizeof(Index{}))
	var err error
	//创建并打开geom和index临时文件
	for i := 0; i < splits; i++ {
		geomfiles[i], err = ioutil.TempFile(tmpdir, "geom.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(geomfiles[i].Name())
		indexfiles[i], err = ioutil.TempFile(tmpdir, "index.*")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(indexfiles[i].Name())

		*availfiles -= 4
	}
	//每个输入读取器对应的geom和index划分为splits个子文件 inputs==cpus，关闭geomfds和indexfds
	for i := 0; i < inputs; i++ {
		// geomst, err := geomfilesIn[i].Stat()
		// if err != nil {
		// 	log.Fatal(err)
		// }
		indexst, err := indexfilesIn[i].Stat()
		if err != nil {
			log.Fatal(err)
		}
		ixlen := indexst.Size()
		if ixlen != 0 {
			geommap, err := gommap.Map(geomfilesIn[i].Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
			if err != nil {
				log.Fatal(err)
			}
			indexmap, err := gommap.Map(indexfilesIn[i].Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
			if err != nil {
				log.Fatal(err)
			}
			nIndex := int(ixlen / int64(ixsize))
			for j := 0; j < nIndex; j++ {
				ix := *(*Index)(unsafe.Pointer(&indexmap[j*ixsize]))
				fmt.Println(ix)
				//which>=0 && which<128 ???划分依据，index中的geomboxcenter→indexcode
				which := ix.Ix << prefix >> (64 - splitbits)
				pos := subGeompos[which]
				// subGeompos[which] += ix.End - ix.Start
				n, err := geomfiles[which].Write(geommap[ix.Start:ix.End])
				if err != nil {
					log.Fatal(err)
				}
				if int64(n) != ix.End-ix.Start {
					log.Fatal("size mismatch")
				}
				atomic.AddInt64(&subGeompos[which], int64(ix.End-ix.Start))
				ix.Start = pos
				ix.End = subGeompos[which]

				n, err = indexfiles[which].Write(indexmap[j*ixsize : j*ixsize+ixsize])
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("index size : ", n)
			}
			err = geommap.UnsafeUnmap()
			if err != nil {
				log.Fatal(err)
			}
			err = indexmap.UnsafeUnmap()
			if err != nil {
				log.Fatal(err)
			}
		}
		*availfiles += 2
	}

	for i := 0; i < splits; i++ {
		err := geomfiles[i].Close()
		if err != nil {
			log.Fatal(err)
		}
		err = indexfiles[i].Close()
		if err != nil {
			log.Fatal(err)
		}
		*availfiles += 2
	}

	for i := 0; i < splits; i++ {
		alreadClosed := false
		geomst, err := geomfiles[i].Stat()
		if err != nil {
			log.Fatal(err)
		}
		geomslen := geomst.Size()
		indexst, err := indexfiles[i].Stat()
		if err != nil {
			log.Fatal(err)
		}
		ixslen := indexst.Size()
		if ixslen > 0 {
			if ixslen+geomslen < mem {
				indexpos := ixslen
				page := int64(os.Getpagesize())
				var maxUnit int64 = 2 * 1024 * 1024 * 1024
				unit := ((indexpos/int64(cpus) + int64(ixsize) - 1) / int64(ixsize)) * int64(ixsize)
				if unit > maxUnit {
					unit = maxUnit
				}
				unit = ((unit + page - 1) / page) * page
				if unit < page {
					unit = page
				}
				//进行归并排序处理，多线程
				nmerges := (indexpos + unit - 1) / unit
				merges := make([]MergeList, nmerges)
				fmt.Println(len(merges))
				// ① sort
				msort := func(task int) {
					var start int64
					for start = int64(task) * unit; start < indexpos; start += unit * int64(cpus) {
						end := start + unit
						if end > indexpos {
							end = indexpos
						}
						merges[start/unit].Start = start
						merges[start/unit].End = end
						merges[start/unit].Next = nil

						mmap, err := gommap.MapRegion(indexfile.Fd(), start, end-start, gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_PRIVATE)
						if err != nil {
							log.Fatal("mmap error for sort features")
						}
						fmt.Println(len(mmap))

						quickSortIndexBuf(mmap, ixsize)

						//map again to avoids disk access

					}

				}
				for i := 0; i < cpus; i++ {
					msort(i)
				}
				// ② Merge(merges,nmerges)
				indexmap, err := gommap.Map(indexfiles[i].Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(len(indexmap))
				// madvise random
				// madvise willneed
				geomap, err := gommap.Map(geomfiles[i].Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(len(geomap))

				// madvise random
				// madvise willneed
				mergeSplits(merges, indexmap, indexfile, geomap, geomfile, geomposOut, progress, progressMax, progressReported, maxzoom, gamma, ds)

			} else if ixslen == int64(ixsize) || prefix+int(splitbits) >= 64 {
				indexmap, err := gommap.Map(indexfiles[i].Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(len(indexmap))
				geomap, err := gommap.Map(geomfiles[i].Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(len(geomap))
				var a int64
				for a = 0; a < ixslen; a += int64(ixsize) {
					idx := (*Index)(unsafe.Pointer(&indexmap[a]))
					pos := *geomposOut
					n, err := geomfile.Write(geomap[idx.Start : idx.End-idx.Start])
					if err != nil {
						log.Fatal(err)
					}
					atomic.AddInt64(geomposOut, int64(n))

					minzoom := CalcFeatureMinzoom(idx, ds, maxzoom, gamma)
					n, err = geomfile.Write([]byte{byte(int8(minzoom))})
					if err != nil {
						log.Fatal(err)
					}
					atomic.AddInt64(geomposOut, int64(n))

					*progress += (idx.End - idx.Start) * 3 / 4
					if !quiet && quietProgress == 0 && progressTime() && (100**progress / *progressMax != *progressReported) {
						log.Printf("Reordering geomtry: %d %% \r", 100**progress / *progress)
						*progressReported = 100 * *progress / *progress
					}

					idx.Start = pos
					idx.End = *geomposOut
					n, err = indexfile.Write(indexmap[a : a+int64(ixsize)])
					if err != nil {
						log.Fatal(err)
					}
				}
			} else {

				*progressMax += geomst.Size() / 4

				radix1(geomfiles[i:i+1], indexfiles[i:i+1], 1, prefix+int(splitbits), *availfiles/4, mem, tmpdir, availfiles, geomfile, indexfile, geomposOut, progress, progressMax, progressReported, maxzoom, basezoom, dorprate, gamma, ds)

				alreadClosed = true
			}
		}

		if !alreadClosed {
			err := geomfiles[i].Close()
			if err != nil {
				log.Fatal(err)
			}
			err = indexfiles[i].Close()
			if err != nil {
				log.Fatal(err)
			}
			*availfiles += 2
		}
	}

}

func prepDropStates(maxzoom, basezoom int, droprate float64) []DropState {
	ds := make([]DropState, maxzoom+1)
	for i := 0; i <= maxzoom; i++ {
		ds[i].Gap = 0
		ds[i].PrevIndex = 0
		ds[i].Interval = 0

		if i < basezoom {
			ds[i].Interval = math.Exp(math.Log(droprate) * float64(basezoom-i))
		}
		x := 1 << uint(64-2*(i+8))
		ds[i].Scale = float64(x)
		ds[i].Seq = 0
		ds[i].Included = 0
		ds[i].X = 0
		ds[i].Y = 0
	}
	return ds
}

func partitionIndexBuf(a []byte, s int) int {
	n := len(a) / s
	hi := (n - 1) * s
	pivot := (*Index)(unsafe.Pointer(&a[hi]))
	i := -1
	tmpbuf := make([]byte, s)
	for j := 0; j < n; j++ {
		js := j * s
		aj := (*Index)(unsafe.Pointer(&a[js]))
		if aj.Ix < pivot.Ix {
			i++
			is := i * s
			copy(tmpbuf, a[js:js+s])
			copy(a[js:js+s], a[is:is+s])
			copy(a[is:is+s], tmpbuf)
		} else if aj.Ix == pivot.Ix {
			if aj.Seq < pivot.Seq {
				i++
				is := i * s
				copy(tmpbuf, a[js:js+s])
				copy(a[js:js+s], a[is:is+s])
				copy(a[is:is+s], tmpbuf)
			}
		}
	}
	lo := (i + 1) * s
	copy(tmpbuf, a[lo:lo+s])
	copy(a[lo:lo+s], a[hi:hi+s])
	copy(a[hi:hi+s], tmpbuf)
	return lo
}

func quickSortIndexBuf(a []byte, s int) {
	if len(a) <= s {
		return
	}
	p := partitionIndexBuf(a, s)
	if p-s > 0 { //-s → 只有第一个元素，无需再排序
		quickSortIndexBuf(a[:p], s)
	}
	if p+s+s < len(a) { //+s → 最后一个元素无需再排序
		quickSortIndexBuf(a[p+s:], s)
	}
}

func mergeSplits(merges []MergeList, indexmap []byte, indexfile *os.File, geomap []byte, geomfile *os.File, geompos, progress, progressMax, progressReported *int64, maxzoom int, gamma float64, ds []DropState) {
	var head *MergeList
	for i := range merges {
		if merges[i].Start < merges[i].End {
			insert(&(merges[i]), &head, indexmap) //备注
		}
	}
	lastProgress = 0

	for head != nil {
		idx := (*Index)(unsafe.Pointer(&indexmap[head.Start]))
		pos := *geompos
		n, err := geomfile.Write(geomap[idx.Start : idx.End-idx.Start])
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))

		minzoom := CalcFeatureMinzoom(idx, ds, maxzoom, gamma)
		n, err = geomfile.Write([]byte{byte(int8(minzoom))})
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(geompos, int64(n))

		*progress += (idx.End - idx.Start) * 3 / 4
		if !quiet && quietProgress != 0 && progressTime() && (100**progress / *progressMax != *progressReported) {
			log.Printf("Reordering geomtry: %d %% \r", 100**progress / *progress)
			*progressReported = 100 * *progress / *progress
		}

		idx.Start = pos
		idx.End = *geompos
		s := unsafe.Sizeof(Index{})
		indexfile.Write(indexmap[head.Start : head.Start+int64(s)])
		head.Start += int64(s)

		m := head
		head = m.Next
		m.Next = nil

		if m.Start < m.End {
			insert(m, &head, indexmap)
		}

	}

}

func insert(m *MergeList, head **MergeList, indexmap []byte) {
	for *head != nil && indexcmp(indexmap[m.Start:], indexmap[(*head).Start:]) > 0 {
		head = &((*head).Next)
	}
	m.Next = *head
	*head = m
}

func indexcmp(v1, v2 []byte) int {
	idx1 := (*Index)(unsafe.Pointer(&v1[0]))
	idx2 := (*Index)(unsafe.Pointer(&v2[0]))

	if idx1.Ix < idx2.Ix {
		return -1
	} else if idx1.Ix > idx2.Ix {
		return 1
	}

	if idx1.Seq < idx2.Seq {
		return -1
	} else if idx1.Seq > idx2.Seq {
		return 1
	}
	return 0
}

//CalcFeatureMinzoom 计算要素的最小缩放级别
func CalcFeatureMinzoom(idx *Index, ds []DropState, maxzoom int, gamma float64) (minzoom int) {
	// xx, yy := DecodeQuadkey(idx.Ix)
	if gamma >= 0 &&
		(idx.T == vtPoint ||
			(additional[aLineDrop] != 0 && idx.T == vtLine) ||
			(additional[aPolygonDrop] != 0 && idx.T == vtPolygon)) {

		for i := maxzoom; i >= 0; i-- {
			ds[i].Seq++
		}
		for i := maxzoom; i >= 0; i-- {
			if ds[i].Seq >= 0 {
				ds[i].Seq -= ds[i].Interval
				ds[i].Included++
			} else {
				minzoom = i + 1
				break
			}
		}
	}
	return minzoom
}

func progressTime() bool {
	if progressInterval == 0 {
		return true
	}
	now := time.Now().Second()
	if float64(now)-lastProgress >= progressInterval {
		lastProgress = float64(now)
		return true
	}
	return false
}
