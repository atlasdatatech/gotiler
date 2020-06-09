package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

//ParseGeoCSV 处理csv格式数据，只支持点类型数据
func ParseGeoCSV(sst []SerializationState, file *os.File, layer int, layerName string) error {
	if file == nil {
		file = os.Stdin
	}
	s := ""
	fmt.Println(s)
	ix, iy := GetGeomCol(file)
	if ix < 0 || iy < 0 {
		log.Fatalf(`找不到%s的 "x", "lon", "longitude", "经度" 等空间坐标字段`, file.Name())
	}
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return err
	}
	fmt.Println(headers)
	rownum := 0
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		rownum++
		if err != nil {
			log.Warningf("%s: reader line(%d) failed, error: %s", file.Name(), rownum, err)
			continue
		}

		if len(row[ix]) == 0 || len(row[iy]) == 0 {
			log.Warningf("%s: line(%d) nil geomtry.", file.Name(), rownum)
			continue
		}

		lon, _ := strconv.ParseFloat(row[ix], 64)
		lat, _ := strconv.ParseFloat(row[iy], 64)
		fmt.Println("lon: ", lon, ", lat: ", lat)
		//project
		x, y := projection.Project(lon, lat, 32)
		fmt.Println("x: ", x, ", y: ", y)
		//draw vector
		draw := Draw{
			Opt: vtMoveto,
			X:   x,
			Y:   y,
		}
		dv := DrawVec{draw}
		fmt.Println(dv)
		//attribute
		fullKeys := []string{}
		fullValues := []SerialVal{}
		fmt.Println(fullKeys, fullValues)
		for i, c := range row {
			if i != ix && i != iy {
				sv := SerialVal{}
				num, err := strconv.ParseFloat(c, 64)
				if err == nil {
					sv.Type = mvtDouble
					fmt.Println(num)
				} else if len(c) == 0 && prevent[pEmptyCSVColumns] != 0 {
					sv.Type = mvtNull
					c = "null"
				} else {
					sv.Type = mvtString
				}
				fmt.Println(c)
				sv.S = c
				fullKeys = append(fullKeys, headers[i])
				fullValues = append(fullValues, sv)
			}
		}
		//serialize
		sf := &SerialFeature{}
		sf.Layer = layer
		sf.LayerName = layerName
		sf.Segment = sst[0].Segment
		sf.HasID = false
		sf.ID = 0
		sf.HasMinzoom = false
		sf.HasMaxzoom = false
		sf.FeatureMinzoom = 0
		sf.Seq = *sst[0].LayerSeq
		sf.geometry = dv
		sf.T = 1
		sf.FullKeys = fullKeys
		sf.FullValues = fullValues
		SerializeFeature(&sst[0], sf)
	}

	return nil
}

//GetGeomCol 获取空间字段
func GetGeomCol(file *os.File) (ix, iy int) {
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return -1, -1
	}

	var records [][]string
	var rowNum, preNum int
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if preNum < 7 {
			records = append(records, row)
			preNum++
		}
		rowNum++
	}

	getColumn := func(cols []string, fields []string) (int, string) {
		for _, c := range cols {
			for i, n := range fields {
				if c == strings.ToLower(n) {
					return i, n
				}
			}
		}
		return -1, ""
	}

	detechColumn := func(min float64, max float64) (int, string) {
		for i, name := range headers {
			num := 0
			for _, row := range records {
				f, err := strconv.ParseFloat(row[i], 64)
				if err != nil || f < min || f > max {
					break
				}
				num++
			}
			if num == len(records) {
				return i, name
			}
		}
		return -1, ""
	}

	xcols := []string{"x", "lon", "longitude", "经度"}
	ix, _ = getColumn(xcols, headers)
	if ix < 0 {
		ix, _ = detechColumn(73, 135)
	}
	ycols := []string{"y", "lat", "latitude", "纬度"}
	iy, _ = getColumn(ycols, headers)
	if iy < 0 {
		iy, _ = detechColumn(18, 54)
	}
	return
}
