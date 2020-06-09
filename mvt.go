package main

type mvtOperation uint8

const (
	mvtMoveto    mvtOperation = 1
	mvtLineto    mvtOperation = 2
	mvtClosepath mvtOperation = 7
)

const (
	mvtString = iota
	mvtFloat
	mvtDouble
	mvtInt
	mvtUint
	mvtSint
	mvtBool
	mvtNull
)

type mvtGeometryType uint8

const (
	mvtPoint      mvtGeometryType = 1
	mvtLinestring mvtGeometryType = 2
	mvtPolygon    mvtGeometryType = 3
)

//MVTGeometry xx
type MVTGeometry struct {
	X, Y int64
	Opt  mvtOperation
}

//MVTFeature xx
type MVTFeature struct {
	ID       uint64
	Type     mvtGeometryType
	HasID    bool
	Dropped  bool
	Tags     []uint
	Geometry []MVTGeometry
}

//MVTValue xx
type MVTValue struct {
	Type   int
	SValue string
	NValue string
}

//MVTLayer xx
type MVTLayer struct {
	Version  int
	Name     string
	Extent   int64
	Features []MVTFeature
	Keys     []string
	Values   []MVTValue
	Keymap   map[string]int
	Valuemap map[MVTValue]int
}

//MVTTile xx
type MVTTile struct {
	Layers []MVTLayer
}
