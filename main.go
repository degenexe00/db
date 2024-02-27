package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/MichalPitr/db_from_scratch/pkg/cli"
	"github.com/MichalPitr/db_from_scratch/pkg/constants"
	"github.com/MichalPitr/db_from_scratch/pkg/types"
)

type Table struct {
	pager       *Pager
	rootPageNum uint32
}

type Pager struct {
	file       *os.File
	fileLength uint32
	numPages   uint32
	pages      [constants.TableMaxPages]*types.Page
}

type Cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool // Indicates position one past the last element.
}

func tableStart(table *Table) *Cursor {
	// Looks for the smallest allowed id. Returns the smallest actual id >= 0.
	cursor := tableFind(table, 0)

	node := getPage(table.pager, cursor.pageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	cursor.endOfTable = numCells == 0
	return cursor
}

func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := getPage(table.pager, rootPageNum)
	nodeType := getNodeType(rootNode)

	if nodeType == types.NodeLeaf {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		return internalNodeFind(table, rootPageNum, key)
	}
}

// Returns the index of the child which should contain the given key.
func internalNodeFindChild(node []byte, key uint32) uint32 {
	numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	// Binary search
	minIdx := uint32(0)
	maxIdx := numKeys

	for minIdx != maxIdx {
		midIdx := (maxIdx-minIdx)/2 + minIdx // mid without overflow
		keyToRight := binary.LittleEndian.Uint32(internalNodeKey(node, midIdx))
		if keyToRight >= key {
			maxIdx = midIdx
		} else {
			minIdx = midIdx + 1
		}
	}
	return minIdx
}

func internalNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := getPage(table.pager, pageNum)

	childIdx := internalNodeFindChild(node, key)
	childNum := binary.LittleEndian.Uint32(internalNodeChild(node, childIdx))
	child := getPage(table.pager, childNum)
	t := getNodeType(child)
	if t == types.NodeLeaf {
		return leafNodeFind(table, childNum, key)
	}
	return internalNodeFind(table, childNum, key)
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := getPage(table.pager, pageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))

	cursor := Cursor{
		table:   table,
		pageNum: pageNum,
	}

	// Binary search
	minIdx := uint32(0)
	onePastMaxIdx := numCells
	for onePastMaxIdx != minIdx {
		midIdx := (onePastMaxIdx-minIdx)/2 + minIdx // mid without overflow
		keyAtIdx := binary.LittleEndian.Uint32(leafNodeKey(node, midIdx))
		if key == keyAtIdx {
			cursor.cellNum = midIdx
			return &cursor
		}
		if key < keyAtIdx {
			onePastMaxIdx = midIdx
		} else {
			minIdx = midIdx + 1
		}
	}

	cursor.cellNum = minIdx
	return &cursor
}

func getNodeType(node []byte) types.NodeType {
	return types.NodeType(node[constants.NodeTypeOffset])
}

func setNodeType(node []byte, nt types.NodeType) {
	node[constants.NodeTypeOffset] = byte(nt)
}

func leafNodeNumCells(node []byte) []byte {
	return node[constants.LeafNodeNumCellsOffset : constants.LeafNodeNumCellsOffset+constants.LeafNodeNumCellsSize]
}

func leafNodeNextLeaf(node []byte) []byte {
	return node[constants.LeafNodeNextLeafOffset : constants.LeafNodeNextLeafOffset+constants.LeafNodeNextLeafSize]
}

func leafNodeCell(node []byte, cellNum uint32) []byte {
	offset := constants.LeafNodeHeaderSize + cellNum*constants.LeafNodeCellSize
	return node[offset : offset+constants.LeafNodeCellSize]
}

func leafNodeKey(node []byte, cellNum uint32) []byte {
	return leafNodeCell(node, cellNum)
}

func leafNodeValue(node []byte, cellNum uint32) []byte {
	return leafNodeCell(node, cellNum)[constants.LeafNodeKeySize : constants.LeafNodeKeySize+constants.LeafNodeValueSize]
}

func initializeLeafNode(node []byte) {
	setNodeType(node, types.NodeLeaf)
	setNodeRoot(node, false)
	binary.LittleEndian.PutUint32(leafNodeNumCells(node), 0)
	binary.LittleEndian.PutUint32(leafNodeNextLeaf(node), 0) // 0 represents no sibling
}

func initializeInternalNode(node []byte) {
	setNodeType(node, types.NodeInternal)
	setNodeRoot(node, false)
	binary.LittleEndian.PutUint32(internalNodeNumKeys(node), 0)
	/*
		Necessary because the root page number is 0. By not initializing the internal node's
		right child to an invalid page number, we may end up with 0 as the node's right child,
		which makes the node the parent of the root.
	*/
	binary.LittleEndian.PutUint32(internalNodeRightChild(node), constants.InvalidPageNum)
}

func internalNodeNumKeys(node []byte) []byte {
	return node[constants.InternalNodeNumKeysOffset : constants.InternalNodeNumKeysOffset+constants.InternalNodeNumKeysSize]
}

func internalNodeRightChild(node []byte) []byte {
	return node[constants.InternalNodeRightChildOffset : constants.InternalNodeRightChildOffset+constants.InternalNodeRightChildSize]
}

func internalNodeCell(node []byte, cellNum uint32) []byte {
	offset := constants.InternalNodeHeaderSize + cellNum*constants.InternalNodeCellSize
	res := node[offset : offset+constants.InternalNodeCellSize]
	return res
}

func internalNodeChild(node []byte, childNum uint32) []byte {
	numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	if childNum > numKeys {
		log.Fatalf("Tried to access childNum %d > numKeys %d\n", childNum, numKeys)
	} else if childNum == numKeys {
		rightChild := internalNodeRightChild(node)
		rightChildNum := binary.LittleEndian.Uint32(rightChild)
		if rightChildNum == constants.InvalidPageNum {
			log.Fatal("Tried to access right child of node, but it was invalid page.")
		}
		return rightChild
	}

	child := internalNodeCell(node, childNum)
	childPageNum := binary.LittleEndian.Uint32(child)
	if childPageNum == constants.InvalidPageNum {
		log.Fatalf("Tried to access child %d of node, but it was invalid page.", childPageNum)
	}
	return child
}

func internalNodeKey(node []byte, keyNum uint32) []byte {
	return internalNodeCell(node, keyNum)[constants.InternalNodeChildSize:]
}

// nodeParent returns the bytes containing the page number of this node's parent
func nodeParent(node []byte) []byte {
	return node[constants.ParentPointerOffset : constants.ParentPointerOffset+constants.ParentPointerSize]
}

func updateInternalNodeKey(node []byte, oldKey uint32, newKey uint32) {
	oldChildIdx := internalNodeFindChild(node, oldKey)
	binary.LittleEndian.PutUint32(internalNodeKey(node, oldChildIdx), newKey)
}

func isNodeRoot(node []byte) bool {
	value := uint8(node[constants.IsRootOffset])
	return value == 1
}

func setNodeRoot(node []byte, isRoot bool) {
	if isRoot {
		node[constants.IsRootOffset] = 1
	} else {
		node[constants.IsRootOffset] = 0
	}
}

func getNodeMaxKey(pager *Pager, node []byte) uint32 {
	if getNodeType(node) == types.NodeLeaf {
		numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
		return binary.LittleEndian.Uint32(leafNodeKey(node, numCells-1))
	}
	rightChildPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(node))
	rightChild := getPage(pager, rightChildPageNum)
	return getNodeMaxKey(pager, rightChild)
}

// Until we start recycling free pages, new pages will always go onto the end of the db file.
func getUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

func internalNodeSplitAndInsert(table *Table, parentPageNum uint32, childPageNum uint32) {
	oldPageNum := parentPageNum
	oldNode := getPage(table.pager, parentPageNum)
	oldMax := getNodeMaxKey(table.pager, oldNode)

	child := getPage(table.pager, childPageNum)
	childMax := getNodeMaxKey(table.pager, child)

	newPageNum := getUnusedPageNum(table.pager)

	/*
	  Declaring a flag before updating pointers which
	  records whether this operation involves splitting the root -
	  if it does, we will insert our newly created node during
	  the step where the table's new root is created. If it does
	  not, we have to insert the newly created node into its parent
	  after the old node's keys have been transferred over. We are not
	  able to do this if the newly created node's parent is not a newly
	  initialized root node, because in that case its parent may have existing
	  keys aside from our old node which we are splitting. If that is true, we
	  need to find a place for our newly created node in its parent, and we
	  cannot insert it at the correct index if it does not yet have any keys
	*/

	splittingRoot := isNodeRoot(oldNode)

	var parent []byte
	var newNode []byte
	if splittingRoot {
		createNewRoot(table, newPageNum)
		parent = getPage(table.pager, table.rootPageNum)
		/*
			If we are splitting the root, we need to update the oldNode
			to point to the new root's left child, newPageNum will already
			point to the new root's right child.
		*/
		oldPageNum = binary.LittleEndian.Uint32(internalNodeChild(parent, 0))
		oldNode = getPage(table.pager, oldPageNum)
	} else {
		parent = getPage(table.pager, binary.LittleEndian.Uint32(nodeParent(oldNode)))
		newNode = getPage(table.pager, newPageNum)
		initializeInternalNode(newNode)
	}

	oldNumKeys := internalNodeNumKeys(oldNode)

	curPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(oldNode))
	cur := getPage(table.pager, curPageNum)

	// First put right child into the new node and set right child of node to invalid page number.
	internalNodeInsert(table, newPageNum, curPageNum)
	binary.LittleEndian.PutUint32(nodeParent(cur), newPageNum)
	binary.LittleEndian.PutUint32(internalNodeRightChild(oldNode), constants.InvalidPageNum)
	// For each key until you get to the middle key, move the child to the new node.
	for i := constants.InternalNodeMaxCells - 1; i > constants.InternalNodeMaxCells/2; i-- {
		curPageNum = binary.LittleEndian.Uint32(internalNodeChild(oldNode, i))
		cur = getPage(table.pager, curPageNum)

		internalNodeInsert(table, newPageNum, curPageNum)
		binary.LittleEndian.PutUint32(nodeParent(cur), newPageNum)

		oldNumKeysNum := binary.LittleEndian.Uint32(oldNumKeys)
		binary.LittleEndian.PutUint32(nodeParent(cur), newPageNum)
		binary.LittleEndian.PutUint32(oldNumKeys, oldNumKeysNum-1)
	}

	/*
		Set child before middle key, which is now the highest key, to be the node's right child
		and decrement number of keys.
	*/
	oldNumKeysNum := binary.LittleEndian.Uint32(oldNumKeys)
	copy(internalNodeRightChild(oldNode), internalNodeChild(oldNode, oldNumKeysNum-1))

	/*
		Determine which of the two nodes after the split should contain the child
		and insert it there.
	*/
	maxAfterSplit := getNodeMaxKey(table.pager, oldNode)
	destPageNum := newPageNum
	if childMax < maxAfterSplit {
		destPageNum = oldPageNum
	}

	internalNodeInsert(table, destPageNum, childPageNum)
	binary.LittleEndian.PutUint32(nodeParent(child), destPageNum)

	updateInternalNodeKey(parent, oldMax, getNodeMaxKey(table.pager, oldNode))

	if !splittingRoot {
		parentNum := binary.LittleEndian.Uint32(nodeParent(oldNode))
		internalNodeInsert(table, parentNum, newPageNum)
		copy(nodeParent(newNode), nodeParent(oldNode))
	}
}

// Inserts a new child key pair to parent that corresponds to the child.
func internalNodeInsert(table *Table, parentPageNum uint32, childPageNum uint32) {
	parent := getPage(table.pager, parentPageNum)
	child := getPage(table.pager, childPageNum)

	childMaxKey := getNodeMaxKey(table.pager, child)
	index := internalNodeFindChild(parent, childMaxKey)

	// Increment number of keys in parent.
	originalNumKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(parent))

	if originalNumKeys >= constants.InternalNodeMaxCells {
		internalNodeSplitAndInsert(table, parentPageNum, childPageNum)
		return
	}

	rightChildPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(parent))
	// Internal node with a right child of INVALID_PAGE_NUM is empty.
	if rightChildPageNum == constants.InvalidPageNum {
		binary.LittleEndian.PutUint32(internalNodeRightChild(parent), childPageNum)
		return
	}

	rightChild := getPage(table.pager, rightChildPageNum)
	/*
		If we are laready at the max number of cells for a node, we cannot increment
		before splitting. Incrementing without inserting a new key/child pair
		and immediately calling internalNodeSplitAndInsert hsa the effect of creating
		a new key at (maxCells + 1) with an unitialized value.
	*/
	binary.LittleEndian.PutUint32(internalNodeNumKeys(parent), originalNumKeys+1)

	if childMaxKey > getNodeMaxKey(table.pager, rightChild) {
		// Replace right child.
		binary.LittleEndian.PutUint32(internalNodeChild(parent, originalNumKeys), rightChildPageNum)
		binary.LittleEndian.PutUint32(internalNodeKey(parent, originalNumKeys), getNodeMaxKey(table.pager, rightChild))
		binary.LittleEndian.PutUint32(internalNodeRightChild(parent), childPageNum)
	} else {
		// Make room for a new cell.
		for i := originalNumKeys; i > index; i-- {
			dest := internalNodeCell(parent, i)
			source := internalNodeCell(parent, i-1)
			copy(dest, source)
		}
		// Something changes here for unknown reasons!?
		binary.LittleEndian.PutUint32(internalNodeChild(parent, index), childPageNum)
		binary.LittleEndian.PutUint32(internalNodeKey(parent, index), childMaxKey)
	}
}

/*
Handle splitting the root.

Old root copied to new page, becomes left child.
Address of right child passed in.
Re-initialize root page to contain the new root node.
New root node points to two children.
*/
func createNewRoot(table *Table, rightChildPageNum uint32) {
	root := getPage(table.pager, table.rootPageNum)
	rightChild := getPage(table.pager, rightChildPageNum)
	leftChildPageNum := getUnusedPageNum(table.pager)
	leftChild := getPage(table.pager, leftChildPageNum)

	if getNodeType(root) == types.NodeInternal {
		initializeInternalNode(rightChild)
		initializeInternalNode(leftChild)
	}

	// Old root is copied into left child.
	copy(leftChild, root)
	setNodeRoot(leftChild, false)

	if getNodeType(leftChild) == types.NodeInternal {
		var child []byte
		numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(leftChild))
		for i := uint32(0); i < numKeys; i++ {
			childPageNum := binary.LittleEndian.Uint32(internalNodeChild(leftChild, i))
			child = getPage(table.pager, childPageNum)
			binary.LittleEndian.PutUint32(nodeParent(child), leftChildPageNum)
		}
		rcPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(leftChild))
		child = getPage(table.pager, rcPageNum)
		binary.LittleEndian.PutUint32(nodeParent(child), leftChildPageNum)
	}

	// Root node is a new internal node with one key and two children.
	initializeInternalNode(root)
	setNodeRoot(root, true)
	binary.LittleEndian.PutUint32(internalNodeNumKeys(root), 1)
	binary.LittleEndian.PutUint32(internalNodeChild(root, 0), leftChildPageNum)
	leftChildMaxKey := getNodeMaxKey(table.pager, leftChild)
	binary.LittleEndian.PutUint32(internalNodeKey(root, 0), leftChildMaxKey)
	binary.LittleEndian.PutUint32(internalNodeRightChild(root), rightChildPageNum)
	binary.LittleEndian.PutUint32(nodeParent(leftChild), table.rootPageNum)
	binary.LittleEndian.PutUint32(nodeParent(rightChild), table.rootPageNum)
}

/*
leafNodeSplitAndInsert creates a new node and moves half of the cells over.

Inserts the new value in one of the two nodes.
Updates parent or creates a new parent.
*/
func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *types.Row) {
	oldNode := getPage(cursor.table.pager, cursor.pageNum)
	oldMax := getNodeMaxKey(cursor.table.pager, oldNode)
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, newPageNum)
	initializeLeafNode(newNode)
	copy(nodeParent(newNode), nodeParent(oldNode))
	copy(leafNodeNextLeaf(newNode), leafNodeNextLeaf(oldNode))
	binary.LittleEndian.PutUint32(leafNodeNextLeaf(oldNode), newPageNum)

	// Existing keys should be divided evenly between old (left) and new (right) nodes.
	// Starting from the right, move each key to the correct position.
	for i := int(constants.LeafNodeMaxCells); i >= 0; i-- {
		var destNode = []byte{}
		if uint32(i) >= constants.LeafNodeLeftSplitCount {
			destNode = newNode
		} else {
			destNode = oldNode
		}
		indexWithinNode := uint32(i) % constants.LeafNodeLeftSplitCount
		destination := leafNodeCell(destNode, indexWithinNode)

		if uint32(i) == cursor.cellNum {
			// inserts new row
			// Copy does nothing???
			copy(leafNodeValue(destNode, indexWithinNode), serializeRow(value))
			binary.LittleEndian.PutUint32(leafNodeKey(destNode, indexWithinNode), key)
		} else if uint32(i) > cursor.cellNum {
			copy(destination, leafNodeCell(oldNode, uint32(i)-1))
		} else {
			copy(destination, leafNodeCell(oldNode, uint32(i)))
		}
	}

	// Update cell count on each leaf node
	binary.LittleEndian.PutUint32(leafNodeNumCells(oldNode), constants.LeafNodeLeftSplitCount)
	binary.LittleEndian.PutUint32(leafNodeNumCells(newNode), constants.LeafNodeRightSplitCount)

	if isNodeRoot(oldNode) {
		createNewRoot(cursor.table, newPageNum)
	} else {
		parentPageNum := binary.LittleEndian.Uint32(nodeParent(oldNode))
		newMax := getNodeMaxKey(cursor.table.pager, oldNode)
		parent := getPage(cursor.table.pager, parentPageNum)

		updateInternalNodeKey(parent, oldMax, newMax)
		internalNodeInsert(cursor.table, parentPageNum, newPageNum)
	}
}

func formatNode(node []byte) {
	nt := types.NodeType(node[constants.NodeTypeOffset])
	if nt == types.NodeInternal {
		isRoot := uint8(node[constants.IsRootOffset])
		parentPtr := binary.LittleEndian.Uint32(node[constants.ParentPointerOffset:])
		numKeys := binary.LittleEndian.Uint32(node[constants.InternalNodeNumKeysOffset:])
		rightChildPtr := binary.LittleEndian.Uint32(node[constants.InternalNodeRightChildOffset:])

		fmt.Printf("node type: %d\n", nt)
		fmt.Printf("is root: %d\n", isRoot)
		fmt.Printf("parent ptr: %d\n", parentPtr)
		fmt.Printf("num keys: %d\n", numKeys)
		fmt.Printf("rightChildPtr: %d\n", rightChildPtr)

		ptr := constants.InternalNodeHeaderSize
		for i := uint32(0); i < numKeys; i++ {
			childPtr := binary.LittleEndian.Uint32(node[ptr:])
			childKey := binary.LittleEndian.Uint32(node[ptr+constants.InternalNodeChildSize:])
			fmt.Printf("child ptr: %d - key %d\n", childPtr, childKey)
			ptr += constants.InternalNodeCellSize
		}
	} else {
		isRoot := uint8(node[constants.IsRootOffset])
		parentPtr := binary.LittleEndian.Uint32(node[constants.ParentPointerOffset:])
		numCells := binary.LittleEndian.Uint32(node[constants.LeafNodeNumCellsOffset:])
		nextLeafPageNum := binary.LittleEndian.Uint32(node[constants.LeafNodeNextLeafOffset:])
		fmt.Printf("node type: %d\n", nt)
		fmt.Printf("is root: %d\n", isRoot)
		fmt.Printf("parent ptr: %d\n", parentPtr)
		fmt.Printf("num cells: %d\n", numCells)
		fmt.Printf("next leaf page num: %d\n", nextLeafPageNum)

		i := constants.LeafNodeHeaderSize
		for i+constants.LeafNodeCellSize < constants.PageSize {
			key := binary.LittleEndian.Uint32(node[i:])
			row := deserializeRow(node[i+constants.LeafNodeKeySize:])
			fmt.Printf("key: %d - row %+v\n", key, row)
			i += constants.LeafNodeCellSize
		}
		fmt.Printf("Leaf Node ends at offset %d\n", i)
	}
}

func leafNodeInsert(cursor *Cursor, key uint32, value *types.Row) {
	node := getPage(cursor.table.pager, cursor.pageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	if numCells >= constants.LeafNodeMaxCells {
		leafNodeSplitAndInsert(cursor, key, value)
		return
	}

	if cursor.cellNum < numCells {
		// Make room for a new cell.
		for i := numCells; i > cursor.cellNum; i-- {
			copy(leafNodeCell(node, i), leafNodeCell(node, i-1))
		}
	}
	binary.LittleEndian.PutUint32(leafNodeNumCells(node), numCells+1)
	binary.LittleEndian.PutUint32(leafNodeKey(node, cursor.cellNum), key)
	copy(leafNodeValue(node, cursor.cellNum), serializeRow(value))
}

func (c *Cursor) advance() {
	node := getPage(c.table.pager, c.pageNum)
	c.cellNum++
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	if c.cellNum >= numCells {
		// Advance to the next leaf node.
		nextPageNum := binary.LittleEndian.Uint32(leafNodeNextLeaf(node))
		if nextPageNum == 0 {
			// This is the rightmost leaf.
			c.endOfTable = true
		} else {
			c.pageNum = nextPageNum
			c.cellNum = 0
		}
	}
}

// internalNodeFindKey returns the index of the cell exactly matching the provided key.
// If such key doesn't exist, second return value is false.
func internalNodeFindKey(node []byte, key uint32) (uint32, bool) {
	numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	// Binary search
	minIdx := uint32(0)
	maxIdx := numKeys
	for minIdx != maxIdx {
		midIdx := (maxIdx-minIdx)/2 + minIdx // mid without overflow
		keyToRight := binary.LittleEndian.Uint32(internalNodeKey(node, midIdx))
		if keyToRight >= key {
			maxIdx = midIdx
		} else {
			minIdx = midIdx + 1
		}
	}
	if keyToRight := binary.LittleEndian.Uint32(internalNodeKey(node, minIdx)); keyToRight != key {
		return 0, false
	}

	return minIdx, true
}

func executeDelete(stmt *types.Statement, table *Table) error {
	/*
		1) Find leaf node that should contain the key.
		2) If key not in the node, terminate search as key does not exist.
		3) If it exists, remove the cell
		4) shift all cells to the right to the left by 1
		5) if the number of cells >= minCellsLeafNode, delete terminates here.
		6) Otherwise, must restructure the node by merging with neighbors
		7) TODO: restucturing follows up as a next step.
	*/
	keyToDelete := stmt.RowToDelete
	cursor := tableFind(table, keyToDelete)
	fmt.Println(cursor.cellNum)
	node := getPage(table.pager, cursor.pageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	formatNode(node)

	if cursor.cellNum >= numCells {
		return fmt.Errorf("key %d does not exist", keyToDelete)
	}

	keyAtIndex := binary.LittleEndian.Uint32(leafNodeKey(node, cursor.cellNum))
	if keyAtIndex != keyToDelete {
		return fmt.Errorf("key %d does not exist", keyToDelete)
	}

	row, err := cursor.Value()
	if err != nil {
		return err
	}
	log.Printf("Found row to delete: %v", deserializeRow(row))

	// 2) Move all cells above the deleted row 1 level down.

	for i := cursor.cellNum + 1; i < numCells; i++ {
		copy(leafNodeCell(node, i-1), leafNodeCell(node, i))
	}

	// Update node's cellnum.
	// TODO: handle deleting last cell in node:
	if numCells-1 == 0 {
		log.Printf("Deleted last cell from leaf node.")
		// TODO: remove node
		// Update parent pointers to this node
	}
	binary.LittleEndian.PutUint32(leafNodeNumCells(node), numCells-1)
	newMaxKey := binary.LittleEndian.Uint32(leafNodeKey(node, numCells-2))

	if keyToDelete < newMaxKey {
		// Can stop delete here since parent's key for this node is unchanged.
		return nil
	}

	// TODO:
	// Add underflow node merging support.

	// Update the maxKey in parent.
	for !isNodeRoot(node) {
		parent := getPage(table.pager, binary.LittleEndian.Uint32(nodeParent(node)))
		idx, ok := internalNodeFindKey(parent, keyToDelete)
		if !ok {
			// This key wasn't the max, so can stop delete op here.
			return nil
		}
		// Update parent's key associated with this cell.
		binary.LittleEndian.PutUint32(internalNodeKey(parent, idx), newMaxKey)
		// Update node to be the current parent so that we traverse up towards root.
		node = parent
	}

	return nil
}

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf("  ")
	}
}

func displayTree(pager *Pager, pageNum uint32, indentLevel uint32) {
	node := getPage(pager, pageNum)
	var numKeys, child uint32

	switch getNodeType(node) {
	case types.NodeLeaf:
		numKeys = binary.LittleEndian.Uint32(leafNodeNumCells(node))
		indent(indentLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentLevel + 1)
			fmt.Printf("- %d\n", binary.LittleEndian.Uint32(leafNodeKey(node, i)))
		}
	case types.NodeInternal:
		numKeys = binary.LittleEndian.Uint32(internalNodeNumKeys(node))
		indent(indentLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		// Avoid printing nodes with 0 keys, since then we'd access invalid page.
		if numKeys > 0 {
			for i := uint32(0); i < numKeys; i++ {
				child = binary.LittleEndian.Uint32(internalNodeChild(node, i))
				displayTree(pager, child, indentLevel+1)
				indent(indentLevel + 1)
				fmt.Printf("- key %d\n", binary.LittleEndian.Uint32(internalNodeKey(node, i)))
			}
		}
		child = binary.LittleEndian.Uint32(internalNodeRightChild(node))
		displayTree(pager, child, indentLevel+1)
	}
}

func getPage(pager *Pager, pageNum uint32) []byte {
	if pageNum >= constants.TableMaxPages {
		fmt.Printf("tried to fetch page number out of bounds. %d > %d\n", pageNum, constants.TableMaxPages)
		os.Exit(1)
	}

	if pager.pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file.
		page := types.Page{}
		numPages := pager.fileLength / constants.PageSize

		if pager.fileLength%constants.PageSize != 0 {
			numPages++
		}

		if pageNum < numPages {
			pager.file.Seek(int64(pageNum*constants.PageSize), 0)
			n, err := pager.file.Read(page[:])
			if err != nil {
				fmt.Printf("error reading file: %d\n", n)
				os.Exit(1)
			}
		}

		pager.pages[pageNum] = &page

		if pageNum >= pager.numPages {
			pager.numPages = pageNum + 1
		}
	}
	return pager.pages[pageNum][:]
}

func dbClose(table *Table) error {
	pager := table.pager
	for i := uint32(0); i < pager.numPages; i++ {
		if table.pager.pages[i] == nil {
			continue
		}
		pagerFlush(table.pager, i)
		table.pager.pages[i] = nil
	}

	err := table.pager.file.Close()
	if err != nil {
		return fmt.Errorf("error closing db file: %s", err.Error())
	}
	return nil
}

func (c *Cursor) Value() ([]byte, error) {
	page := getPage(c.table.pager, c.pageNum)
	return leafNodeValue(page, c.cellNum), nil
}

func serializeRow(r *types.Row) []byte {
	buf := make([]byte, constants.RowSize)
	binary.LittleEndian.PutUint32(buf[constants.IdOffset:], r.Id)
	copy(buf[constants.UsernameOffset:], r.Username[:])
	copy(buf[constants.EmailOffset:], r.Email[:])
	return buf
}

func deserializeRow(buf []byte) types.Row {
	r := types.Row{}
	r.Id = binary.LittleEndian.Uint32(buf[:constants.IdSize])
	copy(r.Username[:], buf[constants.UsernameOffset:constants.UsernameOffset+constants.UsernameSize])
	copy(r.Email[:], buf[constants.EmailOffset:constants.EmailOffset+constants.EmailSize])
	return r
}

func executeInsert(stmt *types.Statement, table *Table) error {
	node := getPage(table.pager, table.rootPageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))

	rowToInsert := stmt.RowToInsert
	keyToInsert := rowToInsert.Id
	cursor := tableFind(table, keyToInsert)

	if cursor.cellNum < numCells {
		keyAtIndex := binary.LittleEndian.Uint32(leafNodeKey(node, cursor.cellNum))
		if keyAtIndex == keyToInsert {
			return fmt.Errorf("duplicate key")
		}
	}
	leafNodeInsert(cursor, rowToInsert.Id, &rowToInsert)
	return nil
}

func executeSelect(stmt *types.Statement, table *Table) error {
	cursor := tableStart(table)
	for !cursor.endOfTable {
		rawRow, err := cursor.Value()
		if err != nil {
			return err
		}
		row := deserializeRow(rawRow)
		cli.PrintRow(row)
		cursor.advance()
	}
	return nil
}

func executeStatement(stmt *types.Statement, table *Table) {
	var err error
	switch stmt.StmtType {
	case types.StmtInsert:
		err = executeInsert(stmt, table)
	case types.StmtSelect:
		err = executeSelect(stmt, table)
	case types.StmtDelete:
		err = executeDelete(stmt, table)
	}
	if err != nil {
		fmt.Printf("Error: %v\n", err.Error())
		return
	}
	fmt.Println("Executed.")
}

func pagerOpen(filename string) *Pager {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("Failed to open file:, %v", err)
	}

	stat, err := f.Stat()
	if err != nil {
		log.Fatalf("Failed to get file stats:, %v", err)
	}
	fileSize := stat.Size()
	pager := Pager{
		file:       f,
		fileLength: uint32(fileSize),
		numPages:   uint32(fileSize) / constants.PageSize,
		pages:      [constants.TableMaxPages]*types.Page{},
	}

	if fileSize%int64(constants.PageSize) != 0 {
		log.Fatal("Db file is not a whole number of pages. Corrupt file.\n")
	}
	for i := uint32(0); i < constants.TableMaxPages; i++ {
		pager.pages[i] = nil
	}
	return &pager
}

func pagerFlush(pager *Pager, pageNum uint32) {
	if pager.pages[pageNum] == nil {
		log.Fatal("Tried to flush null page")
	}

	_, err := pager.file.WriteAt(pager.pages[pageNum][:], int64(pageNum*constants.PageSize))
	if err != nil {
		log.Fatalf("Error writing to file: %v", err)
	}
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)
	table := Table{
		rootPageNum: 0,
		pager:       pager,
	}
	if pager.numPages == 0 {
		rootNode := getPage(pager, 0)
		initializeLeafNode(rootNode)
		setNodeRoot(rootNode, true)
	}
	return &table
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Must supply a database filename.")
	}
	table := dbOpen(os.Args[1])
	reader := bufio.NewScanner(os.Stdin)
	commands := map[string]interface{}{
		".help":  cli.DisplayHelp,
		".clear": cli.ClearScreen,
		".btree": func() {
			fmt.Println("Tree:")
			displayTree(table.pager, 0, 0)
		}, // neat hack.
		".constants": cli.DisplayConstants,
	}
	for {
		cli.PrintPrompt()
		reader.Scan()
		text := cli.CleanInput(reader.Text())
		if text[0] == '.' {
			// Handle meta command starting with ".".
			if cmd, ok := commands[text]; ok {
				cmd.(func())()
			} else if strings.EqualFold(text, ".exit") {
				err := dbClose(table)
				if err != nil {
					fmt.Printf("Error: %s\n", err)
				}
				return
			} else {
				cli.HandleCmd(text)
			}
		} else {
			stmt, err := cli.PrepareStatement(text)
			if err != nil {
				fmt.Printf("Error: %v.\n", err)
				continue
			}
			executeStatement(stmt, table)
		}
	}
}
