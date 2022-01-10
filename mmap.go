package cedar

// mmap 为 cedar 提供 initData, addBlock 等接口，使得 cedar 中读取和写入的 array等信息直接 map 到文件中，做到 demanding page
// 输入：mmapDir，存放 mmap 文件的目录。由于要 map 的内容主要是3个slice，因而直接用3个文件映射。
// 输出：initData，addBlock 接口用来初始化相关数据、扩容

import (
	"fmt"
	"math"
	"os"
	"path"
	"syscall"
	"unsafe"
)

const (
	defaultMaxFileSize = 1 << 34                                         // 文件最大为 16G
	defaultMaxSize     = defaultMaxFileSize / int(unsafe.Sizeof(Node{})) // 最大插入10亿个key
	defaultMemMapSize  = 1 << 34                                         // 假设映射的内存大小为 16G

	nodeSize  = int(unsafe.Sizeof(Node{}))
	nInfoSize = int(unsafe.Sizeof(NInfo{}))
	blockSize = int(unsafe.Sizeof(Block{}))
	metaSize  = int(unsafe.Sizeof(MetaInfo{}))

	defaultNodeNumber = 256

	arrayFileName = "array"
	blockFileName = "block" // 头部存cedar里面非slice的信息
	nInfoFileName = "nInfo"
	fileMode      = os.FileMode(0666)
)

// nolint
type MetaInfo struct {
	inited  bool // determine if the file is empty
	Reduced bool
	reject  [257]int

	blocksHeadFull   int // the index of the first 'Full' block, 0 means no 'Full' block
	blocksHeadClosed int // the index of the first 'Closed' block, 0 means no ' Closed' block
	blocksHeadOpen   int // the index of the first 'Open' block, 0 means no 'Open' block

	capacity int
	size     int
	ordered  bool
	maxTrial int //
}

type MMap struct {
	initSize                           int64
	array                              *[defaultMaxSize]Node
	block                              *[defaultMaxSize >> 8]Block
	nInfo                              *[defaultMaxSize]NInfo
	metaInfo                           *MetaInfo
	arrayBytes, blockBytes, nInfoBytes []byte
	arrayFile, blockFile, nInfoFile    *os.File
}

func NewMMap(mmapDir string) *MMap {
	if _, err := os.Stat(mmapDir); err != nil {
		_assert(os.MkdirAll(mmapDir, fileMode) == nil, "mkdir mmapdir fail")
	}
	arrayFile, err := os.OpenFile(path.Join(mmapDir, arrayFileName), os.O_CREATE|os.O_RDWR, fileMode)
	_assert(err == nil, "Open arrayFile fail %v", err)
	blockFile, err := os.OpenFile(path.Join(mmapDir, blockFileName), os.O_CREATE|os.O_RDWR, fileMode)
	_assert(err == nil, "Open blockFile fail %v", err)
	nInfoFile, err := os.OpenFile(path.Join(mmapDir, nInfoFileName), os.O_CREATE|os.O_RDWR, fileMode)
	_assert(err == nil, "Open nInfoFile fail %v", err)
	// if file size isn't align, return error (not all is zero, or key size is not the same)
	db := &MMap{
		arrayFile: arrayFile,
		blockFile: blockFile,
		nInfoFile: nInfoFile,
	}
	i1, _ := arrayFile.Stat()
	i2, _ := blockFile.Stat()
	i3, _ := nInfoFile.Stat()
	nodeNumber := i1.Size() / int64(nodeSize)
	blockNumber := int64(math.Max(float64(int(i2.Size())-metaSize), 0)) / int64(blockSize)
	ninfoNumber := i3.Size() / int64(nInfoSize)
	_assert(nodeNumber>>8 == blockNumber,
		"node number %d and block number %d not align, remove file in path and retry", nodeNumber, blockNumber)
	_assert(nodeNumber == ninfoNumber,
		"node number %d and ninfoNumber %d not align, remove file in path and retry", nodeNumber, ninfoNumber)
	db.initSize = i1.Size() / int64(nodeSize)
	if db.initSize == 0 {
		db.initSize = defaultNodeNumber
		grow(arrayFile, int64(defaultNodeNumber*nodeSize))
		grow(blockFile, int64(defaultNodeNumber>>8*blockSize+metaSize))
		grow(nInfoFile, int64(defaultNodeNumber*nInfoSize))
	}
	db.arrayBytes = mmap(arrayFile)
	db.array = (*[defaultMaxSize]Node)(unsafe.Pointer(&db.arrayBytes[0]))
	db.blockBytes = mmap(blockFile)
	db.metaInfo = (*MetaInfo)(unsafe.Pointer(&db.blockBytes[0]))
	db.block = (*[defaultMaxSize >> 8]Block)(unsafe.Pointer(&db.blockBytes[metaSize]))
	db.nInfoBytes = mmap(nInfoFile)
	db.nInfo = (*[defaultMaxSize]NInfo)(unsafe.Pointer(&db.nInfoBytes[0]))
	return db
}

// initData in Cedar inplace
func (m *MMap) InitData(c *Cedar) {
	c.array = m.array[:m.initSize]
	c.blocks = m.block[:m.initSize>>8]
	c.nInfos = m.nInfo[:m.initSize]
	c.MetaInfo = m.metaInfo
	if !c.inited {
		c.Reduced = true
		c.capacity = defaultNodeNumber
		c.size = defaultNodeNumber
		c.ordered = true
		c.maxTrial = 1
		c.inited = true
		if !c.Reduced {
			c.array[0] = Node{baseV: 0, check: -1}
		} else {
			c.array[0] = Node{baseV: -1, check: -1}
		}
		// make `baseV` point to the previous element, and make `check` point to the next element
		for i := 1; i < defaultNodeNumber; i++ {
			c.array[i] = Node{baseV: -(i - 1), check: -(i + 1)}
		}
		// make them link as a cyclic doubly-linked list
		c.array[1].baseV = -(defaultNodeNumber - 1)
		c.array[defaultNodeNumber-1].check = -1

		c.blocks[0].eHead = 1
		c.blocks[0].init()

		for i := 0; i <= defaultNodeNumber; i++ {
			c.reject[i] = i + 1
		}
	}
	c.mmap = m
}

// addBlock in Cedar inplace
func (m *MMap) AddBlock(c *Cedar) {
	grow(m.arrayFile, int64(c.capacity*nodeSize))
	grow(m.blockFile, int64(metaSize+c.capacity>>8*blockSize))
	grow(m.nInfoFile, int64(c.capacity*nInfoSize))
	c.array = m.array[:c.capacity]
	c.blocks = m.block[:c.capacity>>8]
	c.nInfos = m.nInfo[:c.capacity]
}

func (c *Cedar) Close() {
	if c.inited {
		munmap(c.mmap.arrayBytes)
		munmap(c.mmap.blockBytes)
		munmap(c.mmap.nInfoBytes)
		_assert(c.mmap.arrayFile.Close() == nil, "close file fail")
		_assert(c.mmap.blockFile.Close() == nil, "close file fail")
		_assert(c.mmap.nInfoFile.Close() == nil, "close file fail")
	}
}

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf(msg, v...))
	}
}

func mmap(file *os.File) []byte {
	ab, err := syscall.Mmap(int(file.Fd()), 0, defaultMemMapSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	_assert(err == nil, "failed to mmap %v", err)
	return ab
}

func grow(file *os.File, size int64) {
	if info, _ := file.Stat(); info.Size() >= size {
		return
	}
	_assert(file.Truncate(size) == nil, "failed to truncate, err %v, size %d", file.Truncate(size), size)
}

func munmap(data []byte) {
	_assert(syscall.Munmap(data) == nil, "failed to munmap")
}
