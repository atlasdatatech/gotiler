package main

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"

	log "github.com/sirupsen/logrus"
)

const (
	cmdBits = 3
)

const (
	opSum = iota
	opProduct
	opMean
	opConcat
	opComma
	opMax
	opMin
)

//Task 切片任务
type Task struct {
	FileNo int
	Next   *Task
}

//WriteTileArgs 瓦片生成参数
type WriteTileArgs struct {
	tasks          *Task
	metabase       []byte
	stringpool     []byte
	minDetail      int
	outdb          *sql.DB
	outdir         string
	buffer         int
	geomFile       *os.File
	todo           int64
	along          *int64 //atomic
	gamma          float64
	gammaOut       float64
	childShards    int
	geomSizes      []int64
	midx           *uint //atomic
	midy           *uint //atomic
	maxzoom        int
	minzoom        int
	fullDetail     int
	lowDetail      int
	simplification float64
	most           *int64 //atomic
	metaOff        []int64
	poolOff        []int64
	initialX       []uint
	initialY       []uint
	running        *int //atomic
	err            int
	layermaps      *[]map[string]LayerEntry
	layerUnmaps    *[][]string
	pass           int
	passes         int
	mingap         uint64
	mingapOut      uint64
	minextent      int64
	minextentOut   int64
	fraction       float64
	fractionOut    float64
	prefilter      string
	postfilter     string
	attributeAccum map[string]int
	stillDropping  bool
	wroteZoom      int
	tilingSeg      int
	filter         string
}

//ManageGap xxx
func ManageGap(index uint64, preindex *uint64, scale float64, gamma float64, gap *float64) int {

	if gamma > 0 {
		if *gap > 0 {
			if index == *preindex {
				return 1 // index相同，无法满足间隔要求
			}
			if index < *preindex || math.Exp(math.Log(float64(index-*preindex)/scale)*gamma) >= *gap {
				*gap = 0
			} else {
				return 1
			}
		} else if index >= *preindex {
			*gap = float64(index-*preindex) / scale
			if *gap == 0 {
				return 1
			} else if *gap < 1 {
				return 1
			} else {
				*gap = 0
			}
		}
		*preindex = index
	}

	return 0
}

//TraverseZooms xxx
func TraverseZooms(geomFiles []*os.File, geomSizes []int64, metabase []byte, stringpool []byte, midx, midy *uint,
	maxzoom, minzoom int, outdb *sql.DB, outdir string, tmpDir string, buffer int, gamma float64,
	fullDetail, lowDetail, minDetail int, metaOff, poolOff []int64, initialX, initialY []uint, simplification float64,
	layermaps []map[string]LayerEntry) int {

	lastProgress = 0
	layermapsOff := len(layermaps)
	for i := 0; i < cpus; i++ {
		e := make(map[string]LayerEntry)
		layermaps = append(layermaps, e)
	}

	var layerUnmaps [][]string
	for seg := 0; seg < len(layermaps); seg++ {
		var lnames []string
		for k := range layermaps[seg] {
			lnames = append(lnames, k)
		}
		layerUnmaps = append(layerUnmaps, lnames)
	}
	var i int
	for i = 0; i <= maxzoom; i++ {
		var most int64
		//创建临时文件
		subfiles := make([]*os.File, tempFiles)
		for j := range subfiles {
			f, err := ioutil.TempFile(tmpDir, fmt.Sprintf("geom%d.*", j))
			if err != nil {
				log.Fatal(err)
			}
			subfiles[j] = f
		}

		var usefulThreads int
		var todo int64
		for _, v := range geomSizes {
			todo += v
			if v > 0 {
				usefulThreads++
			}
		}
		//确定线程数
		threads := cpus
		if threads > tempFiles/4 {
			threads = tempFiles / 4
		}
		if threads > usefulThreads {
			threads = usefulThreads
		}
		// Round down to a power of 2
		var e uint
		for e = 0; e < 30; e++ {
			if threads >= (1<<e) && threads < (1<<(e+1)) {
				threads = 1 << e
			}
		}
		if threads >= (1 << 30) {
			threads = 1 << 30
		}
		if threads < 1 {
			threads = 1
		}
		// Assign temporary files to threads

		tasks := make([]Task, tempFiles)

		type dispatch struct {
			tasks *Task
			todo  int64
			next  *dispatch
		}
		dispatches := make([]dispatch, threads)
		disptchHead := &dispatches[0]

		for t := range dispatches {
			if t+1 < len(dispatches) {
				dispatches[t].next = &dispatches[t+1]
			}
		}

		for ii, s := range geomSizes {
			if s == 0 {
				continue
			}
			tasks[ii].FileNo = ii
			tasks[ii].Next = disptchHead.tasks
			disptchHead.tasks = &tasks[ii]
			disptchHead.todo += s

			here := disptchHead
			disptchHead = disptchHead.next
			var dpp **dispatch
			for dpp = &disptchHead; dpp != nil; dpp = &(*dpp).next {
				if here.todo < (*dpp).todo {
					break
				}
			}
			here.next = *dpp
			*dpp = here
		}
		err := MaxInt
		start := 1
		if additional[aIncreaseGammaAsNeeded] != 0 || additional[aDropDensestAsNeeded] != 0 ||
			additional[aCoalesceDensestAsNeeded] != 0 || additional[aClusterDensestAsNeeded] != 0 ||
			additional[aDropFractionAsNeeded] != 0 || additional[aCoalesceFractionAsNeeded] != 0 ||
			additional[aDropSmallestAsNeeded] != 0 || additional[aCoalesceSmallestAsNeeded] != 0 {
			start = 0
		}
		zoomGamma := gamma
		zoomMingap := uint64(((1 << (32 - uint(i))) / 256 * clusterDistance) * ((1 << (32 - uint(i))) / 256 * clusterDistance))
		var zoomMinExtent int64
		var zoomFraction float64 = 1
		for pass := start; pass < 2; pass++ {
			args := make([]WriteTileArgs, threads)
			running := threads
			var along int64
			for t := 0; t < threads; t++ {
				//create threads
				args[t].metabase = metabase
				args[t].stringpool = stringpool
				args[t].minDetail = minDetail

				args[t].outdb = outdb // locked with db_lock
				args[t].outdir = outdir
				args[t].buffer = buffer
				// args[t].fname = fname
				ii := t * (tempFiles / threads)
				args[t].geomFile = geomFiles[ii]
				args[t].todo = todo
				args[t].along = &along // locked with var_lock
				args[t].gamma = zoomGamma
				args[t].gammaOut = zoomGamma
				args[t].mingap = zoomMingap
				args[t].mingapOut = zoomMingap
				args[t].minextent = zoomMinExtent
				args[t].minextentOut = zoomMinExtent
				args[t].fraction = zoomFraction
				args[t].fractionOut = zoomFraction
				args[t].childShards = tempFiles / threads
				args[t].simplification = simplification

				args[t].geomSizes = geomSizes
				args[t].midx = midx // locked with var_lock
				args[t].midy = midy // locked with var_lock
				args[t].maxzoom = maxzoom
				args[t].minzoom = minzoom
				args[t].fullDetail = fullDetail
				args[t].lowDetail = lowDetail
				args[t].most = &most // locked with var_lock
				args[t].metaOff = metaOff
				args[t].poolOff = poolOff
				args[t].initialX = initialX
				args[t].initialY = initialY
				args[t].layermaps = &layermaps
				args[t].layerUnmaps = &layerUnmaps
				args[t].tilingSeg = t + layermapsOff
				// args[t].prefilter = prefilter
				// args[t].postfilter = postfilter
				// args[t].attributeAccum = attributeAccum
				// args[t].filter = filter

				args[t].tasks = dispatches[t].tasks
				args[t].running = &running
				args[t].pass = pass
				args[t].passes = 2 - start
				args[t].wroteZoom = -1
				args[t].stillDropping = false
			}

			for t := 0; t < threads; t++ {
				//jion threads
			}

		}

		if err != MaxInt {
			return err
		}

	}

	//关闭文件,need?
	for _, f := range geomFiles {
		err := f.Close()
		if err != nil {
			log.Fatal()
		}
	}

	if !quiet {
		log.Printf("traverzooms finished \n")
	}

	return maxzoom
}
func writeTile(arg *WriteTileArgs, geomposIn *int64, z int, tx, ty uint, detail, minDetail int, maxZoom, minZoom int) int64 {

	// (FILE *geoms, std::atomic<long long> *geompos_in, char *metabase, char *stringpool, int z,
	// unsigned tx, unsigned ty, int detail, int min_detail, sqlite3 *outdb, const char *outdir,
	// int buffer, const char *fname, FILE **geomfile, int minzoom, int maxzoom, double todo,
	// std::atomic<long long> *along, long long alongminus, double gamma, int child_shards,
	// long long *meta_off, long long *pool_off, unsigned *initial_x, unsigned *initial_y,
	// std::atomic<int> *running, double simplification, std::vector<std::map<std::string,
	// layermap_entry>> *layermaps, std::vector<std::vector<std::string>> *layer_unmaps,
	// size_t tiling_seg, size_t pass, size_t passes, unsigned long long mingap,
	// long long minextent, double fraction, const char *prefilter,
	// const char *postfilter, struct json_object *filter, write_tile_args *arg)

	var lineDetail int
	var mergeFraction float64 = 1
	var mingapFraction float64 = 1
	var minextentFraction float64 = 1

	var progress float64

	log.Println(mergeFraction, mingapFraction, minextentFraction, progress)

	var childShards int
	maxZoomIncrement := int(math.Log(float64(childShards)) / math.Log(4))

	if childShards < 4 || maxZoomIncrement < 1 {
		log.Fatalf("Internal error: %d shards, max zoom increment %d\n", childShards, maxZoomIncrement)
	}
	if (((childShards - 1) << 1) & childShards) != childShards {
		log.Fatalf("Internal error: %d shards not a power of 2\n", childShards)
	}

	nextzoom := z + 1
	if nextzoom < minZoom {
		if z+maxZoomIncrement > minZoom {
			nextzoom = minZoom
		} else {
			nextzoom = z + maxZoomIncrement
		}
	}

	var hasPolygon = false
	var firstTime = true
	log.Println(hasPolygon, firstTime)
	// This only loops if the tile data didn't fit, in which case the detail
	// goes down and the progress indicator goes backward for the next try.
	for lineDetail = detail; lineDetail >= minDetail || lineDetail == detail; lineDetail-- {
		progress = 0

		for {
			var sf SerialFeature

			whichPartial := -1
			log.Println(whichPartial)
			// nextFeature()

			if sf.T < 0 {
				break
			}
		}

	}

	log.Printf("could not make tile %d/%d/%d small enough\n", z, tx, ty)
	return -1

}
func runThread(arg *WriteTileArgs) {

	for task := arg.tasks; task != nil; task = task.Next {

		n := task.FileNo
		if arg.geomFile == nil {
			// only one source file for zoom level 0
			continue
		}

		if arg.geomSizes[n] == 0 {
			continue
		}

		var geompos int64
		var prevgeom int64

		fmt.Println(geompos)
		fmt.Println(prevgeom)

		for {
			var x, y uint
			bufreader := bufio.NewReader(arg.geomFile)
			z, err := binary.ReadVarint(bufreader)
			if err != nil {
				// log.Error(err) //not a error
				break
				// log.Fatal(err)
			}
			lpos := bufreader.Buffered()
			fmt.Println(lpos)
			binary.Read(bufreader, binary.BigEndian, &x)
			binary.Read(bufreader, binary.BigEndian, &y)
			lpos = bufreader.Buffered()

			arg.wroteZoom = int(z)

			// writeTile(arg, z, x, y)

		}

	}

	*(arg.running)--
	return
}

func nextFeature(geoms *os.File, geomposIn *int64) SerialFeature {
	var sf SerialFeature
	return sf
}
