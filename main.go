package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

const cliName string = "simpleREPL"
const dbName string = "simpleDB"

const idSize uint32 = 4
const usernameSize uint32 = 32
const emailSize uint32 = 255
const idOffset uint32 = 0
const usernameOffset uint32 = idOffset + idSize
const emailOffset uint32 = usernameOffset + usernameSize
const rowSize uint32 = idSize + usernameSize + emailSize

const pageSize uint32 = 4096
const tableMaxPages uint32 = 100

// Node Header Layout
const (
	nodeTypeSize         uint32 = 1
	nodeTypeOffset       uint32 = 0
	isRootSize           uint32 = 1
	isRootOffset         uint32 = nodeTypeSize
	parentPointerSize    uint32 = 4
	parentPointerOffset  uint32 = isRootOffset + isRootSize
	commonNodeHeaderSize uint8  = uint8(nodeTypeSize + isRootSize + parentPointerSize)
)

// Leaf Node Header Layout
const (
	leafNodeNumCellsSize   uint32 = 4
	leafNodeNumCellsOffset uint32 = uint32(commonNodeHeaderSize)
	leafNodeNextLeafSize   uint32 = 4
	leafNodeNextLeafOffset uint32 = leafNodeNumCellsOffset + leafNodeNumCellsSize
	leafNodeHeaderSize     uint32 = uint32(commonNodeHeaderSize) + leafNodeNumCellsSize + leafNodeNextLeafSize
)

// Leaf Node Body Layout
const (
	leafNodeKeySize       uint32 = 4
	leafNodeKeyOffset     uint32 = 0
	leafNodeValueSize            = rowSize
	leafNodeValueOffset   uint32 = leafNodeKeyOffset + leafNodeKeySize
	leafNodeCellSize      uint32 = leafNodeKeySize + leafNodeValueSize
	leafNodeSpaceForcells uint32 = pageSize - leafNodeHeaderSize
	leafNodeMaxCells      uint32 = leafNodeSpaceForcells / leafNodeCellSize
)

// Leaf Node Sizes
const (
	leafNodeRightSplitCount uint32 = (leafNodeMaxCells + 1) / 2
	leafNodeLeftSplitCount  uint32 = (leafNodeMaxCells + 1) - leafNodeRightSplitCount
)

// Internal Node Header Layout
const (
	internalNodeNumKeysSize      uint32 = 4
	internalNodeNumKeysOffset           = uint32(commonNodeHeaderSize)
	internalNodeRightChildSize   uint32 = 4
	internalNodeRightChildOffset        = internalNodeNumKeysOffset + internalNodeNumKeysSize
	internalNodeHeaderSize       uint32 = uint32(commonNodeHeaderSize) + internalNodeNumKeysSize + internalNodeRightChildSize
)

// Internal Node Body Layout
const (
	internalNodeKeySize   uint32 = 4
	internalNodeChildSize uint32 = 4
	internalNodeCellSize  uint32 = internalNodeChildSize + internalNodeKeySize
	internalNodeMaxCells  uint32 = 3 // Keep this small for testing.
)

type nodeType uint8

const (
	nodeInternal nodeType = iota
	nodeLeaf
)

type Page [pageSize]byte

type Table struct {
	pager       *Pager
	rootPageNum uint32
}

type Row struct {
	id       uint32
	username [usernameSize]byte
	email    [emailSize]byte
}

type Statement struct {
	stmtType    statementType
	rowToInsert Row
}

type Pager struct {
	file       *os.File
	fileLength uint32
	numPages   uint32
	pages      [tableMaxPages]*Page
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

	if nodeType == nodeLeaf {
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
	if t == nodeLeaf {
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

func getNodeType(node []byte) nodeType {
	return nodeType(node[nodeTypeOffset])
}

func setNodeType(node []byte, nt nodeType) {
	node[nodeTypeOffset] = byte(nt)
}

func leafNodeNumCells(node []byte) []byte {
	return node[leafNodeNumCellsOffset : leafNodeNumCellsOffset+leafNodeNumCellsSize]
}

func leafNodeNextLeaf(node []byte) []byte {
	return node[leafNodeNextLeafOffset : leafNodeNextLeafOffset+leafNodeNextLeafSize]
}

func leafNodeCell(node []byte, cellNum uint32) []byte {
	offset := leafNodeHeaderSize + cellNum*leafNodeCellSize
	return node[offset : offset+leafNodeCellSize]
}

func leafNodeKey(node []byte, cellNum uint32) []byte {
	return leafNodeCell(node, cellNum)
}

func leafNodeValue(node []byte, cellNum uint32) []byte {
	return leafNodeCell(node, cellNum)[leafNodeKeySize : leafNodeKeySize+leafNodeValueSize]
}

func initializeLeafNode(node []byte) {
	setNodeType(node, nodeLeaf)
	setNodeRoot(node, false)
	binary.LittleEndian.PutUint32(leafNodeNumCells(node), 0)
	binary.LittleEndian.PutUint32(leafNodeNextLeaf(node), 0) // 0 represents no sibling
}

func initializeInternalNode(node []byte) {
	setNodeType(node, nodeInternal)
	setNodeRoot(node, false)
	binary.LittleEndian.PutUint32(internalNodeNumKeys(node), 0)
}

func internalNodeNumKeys(node []byte) []byte {
	return node[internalNodeNumKeysOffset : internalNodeNumKeysOffset+internalNodeNumKeysSize]
}

func internalNodeRightChild(node []byte) []byte {
	return node[internalNodeRightChildOffset : internalNodeRightChildOffset+internalNodeRightChildSize]
}

func internalNodeCell(node []byte, cellNum uint32) []byte {
	offset := internalNodeHeaderSize + cellNum*internalNodeCellSize
	res := node[offset : offset+internalNodeCellSize]
	return res
}

func internalNodeChild(node []byte, childNum uint32) []byte {
	numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	if childNum > numKeys {
		log.Fatalf("Tried to access childNum %d > numKeys %d\n", childNum, numKeys)
	} else if childNum == numKeys {
		return internalNodeRightChild(node)
	}
	return internalNodeCell(node, childNum)
}

func internalNodeKey(node []byte, keyNum uint32) []byte {
	return internalNodeCell(node, keyNum)[internalNodeChildSize:]
}

func nodeParent(node []byte) []byte {
	return node[parentPointerOffset : parentPointerOffset+parentPointerSize]
}

func updateInternalNodeKey(node []byte, oldKey uint32, newKey uint32) {
	oldChildIdx := internalNodeFindChild(node, oldKey)
	binary.LittleEndian.PutUint32(internalNodeKey(node, oldChildIdx), newKey)
}

func isNodeRoot(node []byte) bool {
	value := uint8(node[isRootOffset])
	return value == 1
}

func setNodeRoot(node []byte, isRoot bool) {
	if isRoot {
		node[isRootOffset] = 1
	} else {
		node[isRootOffset] = 0
	}
}

func getNodeMaxKey(node []byte) uint32 {
	switch getNodeType(node) {
	case nodeInternal:
		numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node)) - 1
		return binary.LittleEndian.Uint32(internalNodeKey(node, numKeys))
	case nodeLeaf:
		numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node)) - 1
		return binary.LittleEndian.Uint32(leafNodeKey(node, numCells))
	}
	log.Fatalf("Uknown node type: %v", getNodeType(node))
	return 0
}

// Until we start recycling free pages, new pages will always go onto the end of the db file.
func getUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

// Inserts a new child key pair to parent that corresponds to the child.
func internalNodeInsert(table *Table, parentPageNum uint32, childPageNum uint32) {
	parent := getPage(table.pager, parentPageNum)
	child := getPage(table.pager, childPageNum)

	childMaxKey := getNodeMaxKey(child)
	index := internalNodeFindChild(parent, childMaxKey)

	// Increment number of keys in parent.
	originalNumKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(parent))
	binary.LittleEndian.PutUint32(internalNodeNumKeys(parent), originalNumKeys+1)

	if originalNumKeys >= internalNodeMaxCells {
		log.Fatal("Need to implement splitting inernal node.")
		return
	}

	rightChildPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(parent))
	rightChild := getPage(table.pager, rightChildPageNum)

	if childMaxKey > getNodeMaxKey(rightChild) {
		// Replace right child.
		binary.LittleEndian.PutUint32(internalNodeChild(parent, originalNumKeys), rightChildPageNum)
		binary.LittleEndian.PutUint32(internalNodeKey(parent, originalNumKeys), getNodeMaxKey(rightChild))
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

	// Old root is copied into left child.
	copy(leftChild, root)
	setNodeRoot(leftChild, false)

	// Root node is a new internal node with one key and two children.
	initializeInternalNode(root)
	setNodeRoot(root, true)
	binary.LittleEndian.PutUint32(internalNodeNumKeys(root), 1)
	binary.LittleEndian.PutUint32(internalNodeChild(root, 0), leftChildPageNum)
	leftChildMaxKey := getNodeMaxKey(leftChild)
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
func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	oldNode := getPage(cursor.table.pager, cursor.pageNum)
	oldMax := getNodeMaxKey(oldNode)
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, newPageNum)
	initializeLeafNode(newNode)
	copy(nodeParent(newNode), nodeParent(oldNode))
	copy(leafNodeNextLeaf(newNode), leafNodeNextLeaf(oldNode))
	binary.LittleEndian.PutUint32(leafNodeNextLeaf(oldNode), newPageNum)

	// Existing keys should be divided evenly between old (left) and new (right) nodes.
	// Starting from the right, move each key to the correct position.
	for i := int(leafNodeMaxCells); i >= 0; i-- {
		var destNode = []byte{}
		if uint32(i) >= leafNodeLeftSplitCount {
			destNode = newNode
		} else {
			destNode = oldNode
		}
		indexWithinNode := uint32(i) % leafNodeLeftSplitCount
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
	binary.LittleEndian.PutUint32(leafNodeNumCells(oldNode), leafNodeLeftSplitCount)
	binary.LittleEndian.PutUint32(leafNodeNumCells(newNode), leafNodeRightSplitCount)

	if isNodeRoot(oldNode) {
		createNewRoot(cursor.table, newPageNum)
	} else {
		parentPageNum := binary.LittleEndian.Uint32(nodeParent(oldNode))
		newMax := getNodeMaxKey(oldNode)
		parent := getPage(cursor.table.pager, parentPageNum)

		updateInternalNodeKey(parent, oldMax, newMax)
		internalNodeInsert(cursor.table, parentPageNum, newPageNum)
	}
}

func formatNode(node []byte) {
	nt := nodeType(node[nodeTypeOffset])
	if nt == nodeInternal {
		isRoot := uint8(node[isRootOffset])
		parentPtr := binary.LittleEndian.Uint32(node[parentPointerOffset:])
		numKeys := binary.LittleEndian.Uint32(node[internalNodeNumKeysOffset:])
		rightChildPtr := binary.LittleEndian.Uint32(node[internalNodeRightChildOffset:])

		fmt.Printf("node type: %d\n", nt)
		fmt.Printf("is root: %d\n", isRoot)
		fmt.Printf("parent ptr: %d\n", parentPtr)
		fmt.Printf("num keys: %d\n", numKeys)
		fmt.Printf("rightChildPtr: %d\n", rightChildPtr)

		ptr := internalNodeHeaderSize
		for i := uint32(0); i < numKeys; i++ {
			childPtr := binary.LittleEndian.Uint32(node[ptr:])
			childKey := binary.LittleEndian.Uint32(node[ptr+internalNodeChildSize:])
			fmt.Printf("child ptr: %d - key %d\n", childPtr, childKey)
			ptr += internalNodeCellSize
		}
	} else {
		isRoot := uint8(node[isRootOffset])
		parentPtr := binary.LittleEndian.Uint32(node[parentPointerOffset:])
		numCells := binary.LittleEndian.Uint32(node[leafNodeNumCellsOffset:])
		nextLeafPageNum := binary.LittleEndian.Uint32(node[leafNodeNextLeafOffset:])
		fmt.Printf("node type: %d\n", nt)
		fmt.Printf("is root: %d\n", isRoot)
		fmt.Printf("parent ptr: %d\n", parentPtr)
		fmt.Printf("num cells: %d\n", numCells)
		fmt.Printf("next leaf page num: %d\n", nextLeafPageNum)

		i := leafNodeHeaderSize
		for i+leafNodeCellSize < pageSize {
			key := binary.LittleEndian.Uint32(node[i:])
			row := deserializeRow(node[i+leafNodeKeySize:])
			fmt.Printf("key: %d - row %+v\n", key, row)
			i += leafNodeCellSize
		}
		fmt.Printf("Leaf Node ends at offset %d\n", i)
	}
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := getPage(cursor.table.pager, cursor.pageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	if numCells >= leafNodeMaxCells {
		// Node is full.
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
		// Advane to the next leaf node.
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

type statementType int

const (
	stmtInsert statementType = iota
	stmtSelect
)

func printPrompt() {
	fmt.Printf("%v> ", dbName)
}

func displayHelp() {
	fmt.Printf("Welcome to %v! These are the available commands:\n", cliName)
	fmt.Println(".help    - Show available commands")
	fmt.Println(".clear   - Clear the terminal screen")
	fmt.Println(".exit    - Closes your connection to", dbName)
}

func displayConstants() {
	fmt.Println("Constants:")
	fmt.Printf("rowSize: %d\n", rowSize)
	fmt.Printf("commonNodeHeaderSize: %d\n", commonNodeHeaderSize)
	fmt.Printf("leafNodeHeaderSize: %d\n", leafNodeHeaderSize)
	fmt.Printf("leafNodeCellSize: %d\n", leafNodeCellSize)
	fmt.Printf("leafNodeSpaceForCells: %d\n", leafNodeSpaceForcells)
	fmt.Printf("leafNodeMaxCells: %d\n", leafNodeMaxCells)
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
	case nodeLeaf:
		numKeys = binary.LittleEndian.Uint32(leafNodeNumCells(node))
		indent(indentLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentLevel + 1)
			fmt.Printf("- %d\n", binary.LittleEndian.Uint32(leafNodeKey(node, i)))
		}
	case nodeInternal:
		numKeys = binary.LittleEndian.Uint32(internalNodeNumKeys(node))
		indent(indentLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child = binary.LittleEndian.Uint32(internalNodeChild(node, i))
			displayTree(pager, child, indentLevel+1)
			indent(indentLevel + 1)
			fmt.Printf("- key %d\n", binary.LittleEndian.Uint32(internalNodeKey(node, i)))
		}
		child = binary.LittleEndian.Uint32(internalNodeRightChild(node))
		displayTree(pager, child, indentLevel+1)
	}
}

func clearScreen() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func cleanInput(text string) string {
	output := strings.TrimSpace(text)
	output = strings.ToLower(output)
	return output
}

func handleCmd(cmd string) {
	fmt.Printf("Unknown command: %v\n", cmd)
}

func getPage(pager *Pager, pageNum uint32) []byte {
	if pageNum > tableMaxPages {
		fmt.Printf("tried to fetch page number out of bounds. %d > %d\n", pageNum, tableMaxPages)
		os.Exit(1)
	}

	if pager.pages[pageNum] == nil {
		// Cache miss. Allocate memory and load from file.
		page := Page{}
		numPages := pager.fileLength / pageSize

		if pager.fileLength%pageSize != 0 {
			numPages++
		}

		if pageNum < numPages {
			pager.file.Seek(int64(pageNum*pageSize), 0)
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

func serializeRow(r *Row) []byte {
	buf := make([]byte, rowSize)
	binary.LittleEndian.PutUint32(buf[idOffset:], r.id)
	copy(buf[usernameOffset:], r.username[:])
	copy(buf[emailOffset:], r.email[:])
	return buf
}

func deserializeRow(buf []byte) Row {
	r := Row{}
	r.id = binary.LittleEndian.Uint32(buf[:idSize])
	copy(r.username[:], buf[usernameOffset:usernameOffset+usernameSize])
	copy(r.email[:], buf[emailOffset:emailOffset+emailSize])
	return r
}

func prepareStatement(text string) (*Statement, error) {
	if strings.EqualFold(text[:6], "insert") {
		stmt := Statement{
			stmtType:    stmtInsert,
			rowToInsert: Row{},
		}
		var username, email string
		n, err := fmt.Sscanf(text, "insert %d %s %s", &stmt.rowToInsert.id, &username, &email)
		if err != nil {
			return nil, err
		}
		if n < 3 {
			return nil, fmt.Errorf("expected 3 arguments for insert, but got %d", n)
		}

		if len(username) > int(usernameSize) {
			return nil, fmt.Errorf("string is too long")
		}

		if len(email) > int(emailSize) {
			return nil, fmt.Errorf("string is too long")
		}

		copy(stmt.rowToInsert.username[:], []byte(username))
		copy(stmt.rowToInsert.email[:], []byte(email))
		return &stmt, nil
	}
	if strings.EqualFold(text, "select") {
		return &Statement{stmtType: stmtSelect}, nil
	}
	return nil, fmt.Errorf("unknown statement: %v", text)
}

func printRow(row Row) {
	username := string(bytes.Trim(row.username[:], "\x00"))
	email := string(bytes.Trim(row.email[:], "\x00"))
	fmt.Printf("(%d, %s, %s)\n", row.id, username, email)
}

func executeInsert(stmt *Statement, table *Table) error {
	node := getPage(table.pager, table.rootPageNum)
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))

	rowToInsert := stmt.rowToInsert
	keyToInsert := rowToInsert.id
	cursor := tableFind(table, keyToInsert)

	if cursor.cellNum < numCells {
		keyAtIndex := binary.LittleEndian.Uint32(leafNodeKey(node, cursor.cellNum))
		if keyAtIndex == keyToInsert {
			return fmt.Errorf("duplicate key")
		}
	}
	leafNodeInsert(cursor, rowToInsert.id, &rowToInsert)
	return nil
}

func executeSelect(stmt *Statement, table *Table) error {
	cursor := tableStart(table)
	for !cursor.endOfTable {
		rawRow, err := cursor.Value()
		if err != nil {
			return err
		}
		row := deserializeRow(rawRow)
		printRow(row)
		cursor.advance()
	}
	return nil
}

func executeStatement(stmt *Statement, table *Table) {
	var err error
	switch stmt.stmtType {
	case stmtInsert:
		err = executeInsert(stmt, table)
	case stmtSelect:
		err = executeSelect(stmt, table)
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
		numPages:   uint32(fileSize) / pageSize,
		pages:      [tableMaxPages]*Page{},
	}

	if fileSize%int64(pageSize) != 0 {
		log.Fatal("Db file is not a whole number of pages. Corrupt file.\n")
	}
	for i := uint32(0); i < tableMaxPages; i++ {
		pager.pages[i] = nil
	}
	return &pager
}

func pagerFlush(pager *Pager, pageNum uint32) {
	if pager.pages[pageNum] == nil {
		log.Fatal("Tried to flush null page")
	}

	_, err := pager.file.WriteAt(pager.pages[pageNum][:], int64(pageNum*pageSize))
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
		".help":  displayHelp,
		".clear": clearScreen,
		".btree": func() {
			fmt.Println("Tree:")
			displayTree(table.pager, 0, 0)
		}, // neat hack.
		".constants": displayConstants,
	}
	for {
		printPrompt()
		reader.Scan()
		text := cleanInput(reader.Text())
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
				handleCmd(text)
			}
		} else {
			stmt, err := prepareStatement(text)
			if err != nil {
				fmt.Printf("Error: %v.\n", err)
				continue
			}
			executeStatement(stmt, table)
		}
	}
}
