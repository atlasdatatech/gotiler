package main

//常量定义
const (
	vtPoint   int8 = 1
	vtLine    int8 = 2
	vtPolygon int8 = 3

	vtEnd       int8 = 0
	vtMoveto    int8 = 1
	vtLineto    int8 = 2
	vtClosePath int8 = 7
)

//Draw 绘制结构
type Draw struct {
	X         int64
	Y         int64
	Opt       int8
	Necessary int8
}

//DrawVec 定义Draw数组
type DrawVec []Draw

//LessThan a less than b
func (a Draw) LessThan(b Draw) bool {
	if a.Y < b.Y || (a.Y == b.Y && a.X < b.X) {
		return true
	}
	return false
}

//Equal a equal to b
func (a Draw) Equal(b Draw) bool {
	return a.X == b.X && a.Y == b.Y
}

//NotEqual a not equal to b
func (a Draw) NotEqual(b Draw) bool {
	return a.X != b.X || a.Y != b.Y
}
