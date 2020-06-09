package main

import (
	"database/sql"
	"fmt"
	"strconv"

	_ "github.com/mattn/go-sqlite3" // import sqlite3 driver

	log "github.com/sirupsen/logrus"
)

var (
	maxTilestatsAttributes   = 1000
	maxTilestatsSampleValues = 1000
	maxTilestatsValues       = 100
)

//TypeAndString xx
type TypeAndString struct {
	Type   int
	String string
}

//TypeAndStringStats xx
type TypeAndStringStats struct {
	SampleValues []TypeAndString
	Min          float64
	Max          float64
	Type         int
}

//LayerEntry xx
type LayerEntry struct {
	ID          int
	FileKeys    map[string]TypeAndStringStats
	Minzoom     int
	Maxzoom     int
	Description string

	Points   uint
	Lines    uint
	Polygons uint
	Retain   uint // keep for tilestats, even if no features directly here

}

//AddToFileKeys xxx
func AddToFileKeys(fileKeys map[string]TypeAndStringStats, layerName string, val TypeAndString) {
	if val.Type == mvtNull {
		return
	}
	if _, ok := fileKeys[layerName]; !ok {
		fileKeys[layerName] = TypeAndStringStats{}
	}
	fka := fileKeys[layerName]

	if val.Type == mvtDouble {
		fv, err := strconv.ParseFloat(val.String, 64)
		if err != nil {
			log.Error(err)
		}
		if fv < fka.Min {
			fka.Min = fv
		}
		if fv > fka.Max {
			fka.Max = fv
		}
	}
	pos := lowerBound(fka.SampleValues, val)
	if pos == len(fka.SampleValues) || fka.SampleValues[pos] != val {
		front := append(fka.SampleValues[:pos], val)
		fka.SampleValues = append(front, fka.SampleValues[pos:]...)
		if len(fka.SampleValues) > maxTilestatsSampleValues {
			fka.SampleValues = fka.SampleValues[:len(fka.SampleValues)-1]
		}
	}

	fka.Type |= (1 << uint(val.Type))
}

func lowerBound(samples []TypeAndString, val TypeAndString) int {
	end := len(samples)
	left, right := 0, end
	for left < right {
		mid := (right + left) / 2
		if samples[mid].String <= val.String && samples[mid].Type <= val.Type {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if right == end {
		return end
	}
	return right
}

//SetupMBTileTables 初始化配置MBTile库
func mbtilesOpen(mbtilesOut string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", mbtilesOut)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("PRAGMA synchronous=0")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("PRAGMA locking_mode=EXCLUSIVE")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("PRAGMA journal_mode=DELETE")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("create table if not exists tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("create table if not exists metadata (name text, value text);")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("create unique index name on metadata (name);")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("create unique index tile_index on tiles(zoom_level, tile_column, tile_row);")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func mbtilesClose(db *sql.DB) error {
	if db != nil {
		_, err := db.Exec("ANALYZE;")
		if err != nil {
			return err
		}
		err = db.Close()
		if err != nil {
			return err
		}
		return nil
	}
	return nil //fmt.Errorf("db is nil")
}

func mbtilesWriteTile(db *sql.DB, z, tx, ty int, data []byte) error {
	if db != nil {
		_, err := db.Exec("insert into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);", z, tx, 1<<uint(z)-1-ty, data)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("db is nil")
}

func mbtilesWriteMetadata(db *sql.DB) {

}
