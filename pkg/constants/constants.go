package constants

import "math"

const (
	CliName string = "simpleREPL"
	DbName  string = "simpleDB"

	PageSize       uint32 = 4096
	TableMaxPages  uint32 = 100
	InvalidPageNum uint32 = math.MaxUint32

	IdSize         uint32 = 4
	UsernameSize   uint32 = 32
	EmailSize      uint32 = 255
	IdOffset       uint32 = 0
	UsernameOffset uint32 = IdOffset + IdSize
	EmailOffset    uint32 = UsernameOffset + UsernameSize
	RowSize        uint32 = IdSize + UsernameSize + EmailSize
)

// Node Header Layout
const (
	NodeTypeSize         uint32 = 1
	NodeTypeOffset       uint32 = 0
	IsRootSize           uint32 = 1
	IsRootOffset         uint32 = NodeTypeSize
	ParentPointerSize    uint32 = 4
	ParentPointerOffset  uint32 = IsRootOffset + IsRootSize
	CommonNodeHeaderSize uint8  = uint8(NodeTypeSize + IsRootSize + ParentPointerSize)
)

// Leaf Node Header Layout
const (
	LeafNodeNumCellsSize   uint32 = 4
	LeafNodeNumCellsOffset uint32 = uint32(CommonNodeHeaderSize)
	LeafNodeNextLeafSize   uint32 = 4
	LeafNodeNextLeafOffset uint32 = LeafNodeNumCellsOffset + LeafNodeNumCellsSize
	LeafNodeHeaderSize     uint32 = uint32(CommonNodeHeaderSize) + LeafNodeNumCellsSize + LeafNodeNextLeafSize
)

// Leaf Node Body Layout
const (
	LeafNodeKeySize       uint32 = 4
	LeafNodeKeyOffset     uint32 = 0
	LeafNodeValueSize            = RowSize
	LeafNodeValueOffset   uint32 = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize      uint32 = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForcells uint32 = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells      uint32 = LeafNodeSpaceForcells / LeafNodeCellSize
)

// Leaf Node Sizes
const (
	LeafNodeRightSplitCount uint32 = (LeafNodeMaxCells + 1) / 2
	LeafNodeLeftSplitCount  uint32 = (LeafNodeMaxCells + 1) - LeafNodeRightSplitCount
)

// Internal Node Header Layout
const (
	InternalNodeNumKeysSize      uint32 = 4
	InternalNodeNumKeysOffset           = uint32(CommonNodeHeaderSize)
	InternalNodeRightChildSize   uint32 = 4
	InternalNodeRightChildOffset        = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize       uint32 = uint32(CommonNodeHeaderSize) + InternalNodeNumKeysSize + InternalNodeRightChildSize
)

// Internal Node Body Layout
const (
	InternalNodeKeySize   uint32 = 4
	InternalNodeChildSize uint32 = 4
	InternalNodeCellSize  uint32 = InternalNodeChildSize + InternalNodeKeySize
	InternalNodeMaxCells  uint32 = 3 // Keep this small for testing.
)
