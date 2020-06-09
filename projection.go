package main

import (
	"fmt"
	"math"
)

var (
	decodex    [256]uint8
	decodey    [256]uint8
	decodeInit = false
)

var projection Projection

//SetProjection 设置投影
func SetProjection(projName string) error {
	switch projName {
	case "EPSG:4326":
		projection = &EPSG4326{"EPSG:4326", "urn:ogc:def:crs:OGC:1.3:CRS84"}
	case "EPSG:3857":
		projection = &EPSG3857{"EPSG:3857", "urn:ogc:def:crs:EPSG::3857"}
	default:
		return fmt.Errorf("unsupported projection")
	}
	return nil
}

// //IndexCoder xx
// type IndexCoder interface {
// 	EncodeIndex(wx, wy uint64) uint64
// 	DecodeIndex(index uint64) (wx, wy uint64)
// }

//Projection 投影
type Projection interface {
	Project(ix, iy float64, zoom int) (ox, oy int64)
	UnProject(ix, iy int64, zoom int) (ox, oy float64)
}

//EPSG projection info
type EPSG struct {
	Name  string
	Alias string
}

//EPSG4326 plate
type EPSG4326 EPSG

//EPSG3857 webmecarto
type EPSG3857 EPSG

//Project EPSG4326totile
func (proj EPSG4326) Project(lon, lat float64, zoom int) (x, y int64) {

	badLon := false
	if math.IsInf(lon, 0) || math.IsNaN(lon) {
		lon = 720
		badLon = true
	}
	if math.IsInf(lat, 0) || math.IsNaN(lat) {
		lat = 89.9
	}

	if lat < -89.9 {
		lat = -89.9
	}
	if lat > 89.9 {
		lat = 89.9
	}
	if lon < -360 && !badLon {
		lon = -360
	}
	if lon > 360 && !badLon {
		lon = 360
	}

	latRad := lat * math.Pi / 180
	var n int64 = 1 << uint(zoom)

	x = int64(float64(n) * ((lon + 180.0) / 360.0))
	y = int64(float64(n) * (1.0 - (math.Log(math.Tan(latRad)+1.0/math.Cos(latRad)) / math.Pi)) / 2.0)

	return
}

//UnProject tile2EPSG4326
func (proj EPSG4326) UnProject(x, y int64, zoom int) (lon, lat float64) {
	var n int64 = 1 << uint(zoom)
	lon = float64(360.0*x)/float64(n) - 180.0
	lat = math.Atan(math.Sinh(math.Pi*(1-2.0*float64(y)/float64(n)))) * 180.0 / math.Pi
	return
}

//Project EPSG3857totile
func (proj EPSG3857) Project(ix, iy float64, zoom int) (ox, oy int64) {

	if math.IsInf(ix, 0) || math.IsNaN(ix) {
		ix = 40000000.0
	}
	if math.IsInf(iy, 0) || math.IsNaN(iy) {
		iy = 40000000.0
	}

	ox = int64(ix*(1<<31)/6378137.0/math.Pi + (1 << 31))
	oy = int64(((1 << 32) - 1) - (iy*(1<<31)/6378137.0/math.Pi + (1 << 31)))

	if zoom != 0 {
		ox >>= uint(32 - zoom)
		oy >>= uint(32 - zoom)
	}
	return
}

//UnProject tile2EPSG3857
func (proj EPSG3857) UnProject(ix, iy int64, zoom int) (ox, oy float64) {
	if zoom != 0 {
		ix <<= uint(32 - zoom)
		iy <<= uint(32 - zoom)
	}

	ox = float64(ix-(1<<31)) * math.Pi * 6378137.0 / (1 << 31)
	oy = float64((1<<32)-1-iy-(1<<31)) * math.Pi * 6378137.0 / (1 << 31)
	return
}

//EncodeQuadkey 四叉树编码
func EncodeQuadkey(wx, wy uint64) uint64 {
	var index uint64
	var i uint
	for i = 0; i < 32; i++ {
		v := ((wx >> (32 - (i + 1))) & 1) << 1
		v |= (wy >> (32 - (i + 1))) & 1
		v = v << (64 - 2*(i+1))
		index |= v
	}
	return index
}

//DecodeQuadkey 四叉树解码
func DecodeQuadkey(index uint64) (wx, wy uint64) {
	if !decodeInit {
		for ix := 0; ix < 256; ix++ {
			xx, yy := 0, 0
			var i uint8
			for i = 0; i < 32; i++ {
				xx |= ((ix >> (64 - 2*(i+1) + 1)) & 1) << (32 - (i + 1))
				yy |= ((ix >> (64 - 2*(i+1) + 0)) & 1) << (32 - (i + 1))
			}
			decodex[ix] = uint8(xx)
			decodey[ix] = uint8(yy)
		}
		decodeInit = true
	}

	var i uint8
	for i = 0; i < 8; i++ {
		wx |= uint64(decodex[(index>>(8*i))&0xFF]) << (4 * i)
		wy |= uint64(decodey[(index>>(8*i))&0xFF]) << (4 * i)
	}
	return
}

//EncodeHilbert Hilbert编码
func EncodeHilbert(wx, wy uint64) uint64 {
	return HilbertXY2d(1<<32, wx, wy)
}

//DecodeHilbert Hilbert解码
func DecodeHilbert(index uint64) (wx, wy uint64) {

	return HilbertD2xy(1<<32, index)
}

//HilbertRot rote xy
func HilbertRot(n uint64, x, y *uint64, rx, ry uint64) {
	if ry == 0 {
		if rx == 1 {
			*x = n - 1 - *x
			*y = n - 1 - *y
		}
		*x, *y = *y, *x
	}
	return
}

//HilbertXY2d xy2d
func HilbertXY2d(n uint64, x, y uint64) uint64 {
	var d uint64
	var rx, ry uint64
	for s := n / 2; s > 0; s /= 2 {
		if (x & s) != 0 {
			rx = 1
		}
		if (y & s) != 0 {
			ry = 1
		}
		d += s * s * ((3 * rx) ^ ry)
		HilbertRot(s, &x, &y, rx, ry)
	}
	return d
}

//HilbertD2xy d2xy
func HilbertD2xy(n uint64, d uint64) (x, y uint64) {
	var rx, ry uint64
	t := d
	var s uint64
	for s = 1; s < n; s *= 2 {
		rx = 1 & (t / 2)
		ry = 1 & (t ^ rx)
		HilbertRot(s, &x, &y, rx, ry)
		x += s * rx
		y += s * ry
		t /= 4
	}
	return
}
