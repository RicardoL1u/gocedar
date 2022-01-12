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
	useMMap  bool // determine if mmap inited
	LoadSize int  // key size when mmaped
	Reduced  bool
	reject   [257]int

	blocksHeadFull   int // the index of the first 'Full' block, 0 means no 'Full' block
	blocksHeadClosed int // the index of the first 'Closed' block, 0 means no ' Closed' block
	blocksHeadOpen   int // the index of the first 'Open' block, 0 means no 'Open' block

	capacity int
	size     int
	ordered  bool
	maxTrial int //
}

type MMap struct {
	loadSize                           int
	mmapDir                            string
	initSize                           int
	array                              *[defaultMaxSize]Node
	block                              *[defaultMaxSize >> 8]Block
	nInfo                              *[defaultMaxSize]NInfo
	metaInfo                           *MetaInfo
	arrayBytes, blockBytes, nInfoBytes []byte
	arrayFile, blockFile, nInfoFile    *os.File
	arrayMSize, blockMSize, nInfoMSize int
}

func NewMMap(mmapDir string) *MMap {
	if _, err := os.Stat(mmapDir); err != nil {
		_assert(os.MkdirAll(mmapDir, fileMode) == nil, "mkdir mmapdir fail")
	}

	// if file size isn't align, return error (not all is zero, or key size is not the same)
	m := &MMap{
		mmapDir: mmapDir,
	}
	m.OpenFile()
	ai, _ := m.arrayFile.Stat()
	bi, _ := m.blockFile.Stat()
	ni, _ := m.nInfoFile.Stat()

	// check the node number is equal in every file
	nodeNumber := ai.Size() / int64(nodeSize)
	blockNumber := int64(math.Max(float64(int(bi.Size())-metaSize), 0)) / int64(blockSize)
	ninfoNumber := ni.Size() / int64(nInfoSize)
	_assert(nodeNumber>>8 == blockNumber,
		"node number %d and block number %d not align, remove file in path and retry", nodeNumber, blockNumber)
	_assert(nodeNumber == ninfoNumber,
		"node number %d and ninfoNumber %d not align, remove file in path and retry", nodeNumber, ninfoNumber)

	m.initSize = int(ai.Size()) / nodeSize
	if m.initSize == 0 {
		m.initSize = defaultNodeNumber
	} else {
		m.loadSize = m.initSize
	}
	m.allocate(m.initSize)
	return m
}

func (m *MMap) OpenFile() {
	var err error
	m.arrayFile, err = os.OpenFile(path.Join(m.mmapDir, arrayFileName), os.O_CREATE|os.O_RDWR, fileMode)
	_assert(err == nil, "Open arrayFile fail %v", err)
	m.blockFile, err = os.OpenFile(path.Join(m.mmapDir, blockFileName), os.O_CREATE|os.O_RDWR, fileMode)
	_assert(err == nil, "Open blockFile fail %v", err)
	m.nInfoFile, err = os.OpenFile(path.Join(m.mmapDir, nInfoFileName), os.O_CREATE|os.O_RDWR, fileMode)
	_assert(err == nil, "Open nInfoFile fail %v", err)
}

// initData in Cedar inplace
func (m *MMap) InitData(c *Cedar) {
	c.array = m.array[:m.initSize]
	c.blocks = m.block[:m.initSize>>8]
	c.nInfos = m.nInfo[:m.initSize]
	c.MetaInfo = m.metaInfo
	c.mmap = m
	c.LoadSize = m.loadSize
}

// addBlock in Cedar inplace depends on c.capacity
func (m *MMap) AddBlock(c *Cedar, capacity int) {
	m.allocate(capacity)

	c.MetaInfo = m.metaInfo
	c.array = m.array[:c.capacity]
	c.blocks = m.block[:c.capacity>>8]
	c.nInfos = m.nInfo[:c.capacity]
}

// allocate remmap depends on arrayMSize blockMSize nInfoMSize
func (m *MMap) allocate(cap int) {
	// compute memory size
	m.arrayMSize = cap * nodeSize
	m.blockMSize = metaSize + cap>>8*blockSize
	m.nInfoMSize = cap * nInfoSize

	if len(m.arrayBytes) > 0 {
		munmap(m.arrayBytes)
		munmap(m.blockBytes)
		munmap(m.nInfoBytes)
	}

	// grow file to memory size
	grow(m.arrayFile, int64(m.arrayMSize))
	grow(m.blockFile, int64(m.blockMSize))
	grow(m.nInfoFile, int64(m.nInfoMSize))

	// mmap memory
	m.arrayBytes = mmap(m.arrayFile, int(m.arrayMSize))
	m.array = (*[defaultMaxSize]Node)(unsafe.Pointer(&m.arrayBytes[0]))

	m.blockBytes = mmap(m.blockFile, int(m.blockMSize))
	m.metaInfo = (*MetaInfo)(unsafe.Pointer(&m.blockBytes[0]))
	m.block = (*[defaultMaxSize >> 8]Block)(unsafe.Pointer(&m.blockBytes[metaSize]))

	m.nInfoBytes = mmap(m.nInfoFile, int(m.nInfoMSize))
	m.nInfo = (*[defaultMaxSize]NInfo)(unsafe.Pointer(&m.nInfoBytes[0]))
}

func (c *Cedar) Close() {
	if c.useMMap {
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

func mmap(file *os.File, size int) []byte {
	ab, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
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
