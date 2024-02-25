package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/MichalPitr/db_from_scratch/pkg/cli"
	"github.com/MichalPitr/db_from_scratch/pkg/constants"
	"github.com/MichalPitr/db_from_scratch/pkg/types"
)

func TestNewDbRootType(t *testing.T) {
	dbName := "test.db"
	os.Remove(dbName)
	table := dbOpen(dbName)

	// Should have 1 leaf node, 1 page
	if table.pager.numPages != 1 {
		t.Error("Expected new table to have 1 page.")
	}

	node := getPage(table.pager, 0)
	if len(node) != int(constants.PageSize) {
		t.Errorf("Expected page to be 4096 in size.")
	}

	nt := getNodeType(node)
	if nt != types.NodeLeaf {
		t.Errorf("Expected initial node to be leaf node.")
	}

	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	if numCells != 0 {
		t.Error("Expected 0 cells in node.")
	}
}

func TestInsertRow(t *testing.T) {
	dbName := "test.db"
	os.Remove(dbName)
	table := dbOpen(dbName)

	stmt, _ := cli.PrepareStatement("insert 1 user1 user1@example.com")
	executeInsert(stmt, table)

	// Should have 1 leaf page.
	if table.pager.numPages != 1 {
		t.Error("Expected new table to have 2 page.")
	}

	node := getPage(table.pager, 0)
	if len(node) != int(constants.PageSize) {
		t.Errorf("Expected page to be 4096 in size.")
	}

	nt := getNodeType(node)
	if nt != types.NodeLeaf {
		t.Errorf("Expected initial node to be leaf node.")
	}

	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	if numCells != 1 {
		t.Error("Expected 1 cell in node.")
	}
}

func TestInsertSplit(t *testing.T) {
	dbName := "test.db"
	os.Remove(dbName)
	table := dbOpen(dbName)

	// Fill up page, next insert should trigger split.
	for i := 0; i < int(constants.LeafNodeMaxCells); i++ {
		stmt, _ := cli.PrepareStatement(fmt.Sprintf("insert %d user%d user%d@example.com", i, i, i))
		executeInsert(stmt, table)
	}

	// Should have 1 leaf page.
	if table.pager.numPages != 1 {
		t.Error("Expected new table to have 1 page.")
	}

	node := getPage(table.pager, 0)
	if len(node) != int(constants.PageSize) {
		t.Errorf("Expected page to be 4096 in size.")
	}

	nt := getNodeType(node)
	if nt != types.NodeLeaf {
		t.Errorf("Expected initial node to be leaf node.")
	}

	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(node))
	if numCells != constants.LeafNodeMaxCells {
		t.Error("Expected 1 cell in node.")
	}

	// Insert 1 more row to trigger split.
	stmt, _ := cli.PrepareStatement("insert 14 user14 user14@example.com")
	executeInsert(stmt, table)

	// Should have 2 leaf nodes, 1 root internal node.
	if table.pager.numPages != 3 {
		t.Error("Expected table after split to have 2 pages.")
	}

	// Check internal node.
	node = getPage(table.pager, 0)
	nt = getNodeType(node)
	if nt != types.NodeInternal {
		t.Errorf("Expected internal node.")
	}
	numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	if numKeys != 1 {
		t.Errorf("Expected 1 keys in internal node. Got: %d", numKeys)
	}
	rightChildPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(node))
	if rightChildPageNum != 1 {
		t.Errorf("Expected right child page num to be 1. Got: %d", rightChildPageNum)
	}

	leftChildPageNum := binary.LittleEndian.Uint32(internalNodeChild(node, 0))
	if leftChildPageNum != 2 {
		t.Errorf("Expected left child page num to be 2. Got: %d", leftChildPageNum)
	}

	// Check if left child node contains the expected rows:
	leftChild := getPage(table.pager, leftChildPageNum)
	parentNum := binary.LittleEndian.Uint32(nodeParent(leftChild))
	if parentNum != 0 {
		t.Errorf("Child's parent should be 0, got: %d", parentNum)
	}
	nt = getNodeType(leftChild)
	if nt != types.NodeLeaf {
		t.Errorf("Expected leaf node.")
	}
	numCells = binary.LittleEndian.Uint32(leafNodeNumCells(leftChild))
	if numCells != 7 {
		t.Errorf("Expected 7 cells in left child node. Got: %d", numCells)
	}

	// Check if right child node contains the expected rows:
	rightChild := getPage(table.pager, rightChildPageNum)
	parentNum = binary.LittleEndian.Uint32(nodeParent(rightChild))
	if parentNum != 0 {
		t.Errorf("Child's parent should be 0, got: %d", parentNum)
	}
	nt = getNodeType(rightChild)
	if nt != types.NodeLeaf {
		t.Errorf("Expected leaf node.")
	}
	numCells = binary.LittleEndian.Uint32(leafNodeNumCells(rightChild))
	if numCells != 7 {
		t.Errorf("Expected 7 cells in right child node. Got: %d", numCells)
	}
}

func TestInsertSplitUnordered(t *testing.T) {
	dbName := "test.db"
	os.Remove(dbName)
	table := dbOpen(dbName)

	commands := []string{
		"insert 26 user26 user26@example.com",
		"insert 22 user22 user22@example.com",
		"insert 16 user16 user16@example.com",
		"insert 10 user10 user10@example.com",
		"insert 19 user19 user19@example.com",
		"insert 17 user17 user17@example.com",
		"insert 28 user28 user28@example.com",
		"insert 24 user24 user24@example.com",
		"insert 27 user27 user27@example.com",
		"insert 23 user23 user23@example.com",
		"insert 2 user2 user2@example.com",
		"insert 8 user8 user8@example.com",
		"insert 15 user15 user15@example.com",
		"insert 25 user25 user25@example.com",
		"insert 6 user6 user6@example.com",
		"insert 11 user11 user11@example.com",
		"insert 12 user12 user12@example.com",
		"insert 3 user3 user3@example.com",
		"insert 29 user29 user29@example.com",
		"insert 7 user7 user7@example.com",
		"insert 13 user13 user13@example.com",
		"insert 1 user1 user1@example.com",
	}

	for _, cmd := range commands {
		stmt, _ := cli.PrepareStatement(cmd)
		executeInsert(stmt, table)
	}

	// Should have 4 pages:
	if table.pager.numPages != 4 {
		t.Fatalf("Expected new table to have 4 page. Got: %d", table.pager.numPages)
	}

	node := getPage(table.pager, 0)
	if len(node) != int(constants.PageSize) {
		t.Errorf("Expected page to be 4096 in size.")
	}

	nt := getNodeType(node)
	if nt != types.NodeInternal {
		t.Fatalf("Expected root node to be internal node.")
	}

	// 2 keys + right child
	numKeys := binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	if numKeys != 2 {
		t.Fatalf("Expected 2 keys in internal node. Got: %d", numKeys)
	}

	// Verify state is fine.
	pageNum := internalNodeFindChild(node, 14)
	formatNode(node)
	fmt.Println(pageNum)

	// Insert 1 more row to trigger split.
	stmt, _ := cli.PrepareStatement("insert 14 user14 user14@example.com")
	executeInsert(stmt, table)

	// Should have 3 leaf nodes, 1 root internal node.
	if table.pager.numPages != 4 {
		t.Fatalf("Expected table after split to have 4 pages. Got %d", table.pager.numPages)
	}

	// Check internal node.
	node = getPage(table.pager, 0)
	nt = getNodeType(node)
	if nt != types.NodeInternal {
		t.Fatalf("Expected internal node.")
	}

	numKeys = binary.LittleEndian.Uint32(internalNodeNumKeys(node))
	if numKeys != 2 {
		t.Fatalf("Expected 2 keys in internal node. Got: %d", numKeys)
	}
	rightChildPageNum := binary.LittleEndian.Uint32(internalNodeRightChild(node))
	if rightChildPageNum != 1 {
		t.Fatalf("Expected right child page num to be 1. Got: %d", rightChildPageNum)
	}

	firstChildPageNum := binary.LittleEndian.Uint32(internalNodeChild(node, 0))
	if firstChildPageNum != 2 {
		t.Fatalf("Expected left child page num to be 2. Got: %d", firstChildPageNum)
	}

	secondChildPageNum := binary.LittleEndian.Uint32(internalNodeChild(node, 1))
	if secondChildPageNum != 3 {
		t.Fatalf("Expected second child page num to be 3. Got: %d", secondChildPageNum)
	}

	// Check if first child node contains the expected rows:
	firstChild := getPage(table.pager, firstChildPageNum)
	parentNum := binary.LittleEndian.Uint32(nodeParent(firstChild))
	if parentNum != 0 {
		t.Fatalf("Child's parent should be 0, got: %d", parentNum)
	}
	nt = getNodeType(firstChild)
	if nt != types.NodeLeaf {
		t.Fatalf("Expected leaf node.")
	}
	numCells := binary.LittleEndian.Uint32(leafNodeNumCells(firstChild))
	if numCells != 7 {
		t.Fatalf("Expected 7 cells in left child node. Got: %d", numCells)
	}

	// Check if second child node contains the expected rows:
	secondChild := getPage(table.pager, secondChildPageNum)
	parentNum = binary.LittleEndian.Uint32(nodeParent(secondChild))
	if parentNum != 0 {
		t.Fatalf("Child's parent should be 0, got: %d", parentNum)
	}
	nt = getNodeType(secondChild)
	if nt != types.NodeLeaf {
		t.Fatalf("Expected leaf node.")
	}
	numCells = binary.LittleEndian.Uint32(leafNodeNumCells(secondChild))
	if numCells != 8 {
		t.Fatalf("Expected 7 cells in second child node. Got: %d", numCells)
	}

	// Check if right child node contains the expected rows:
	rightChild := getPage(table.pager, rightChildPageNum)
	parentNum = binary.LittleEndian.Uint32(nodeParent(rightChild))
	if parentNum != 0 {
		t.Fatalf("Child's parent should be 0, got: %d", parentNum)
	}
	nt = getNodeType(rightChild)
	if nt != types.NodeLeaf {
		t.Fatalf("Expected leaf node.")
	}
	numCells = binary.LittleEndian.Uint32(leafNodeNumCells(rightChild))
	if numCells != 8 {
		t.Fatalf("Expected 8 cells in right child node. Got: %d", numCells)
	}
}

func TestInsertInternalNodeSplit(t *testing.T) {
	dbName := "test.db"
	os.Remove(dbName)
	table := dbOpen(dbName)

	commands := []string{
		"insert 58 user58 person58@example.com",
		"insert 56 user56 person56@example.com",
		"insert 8 user8 person8@example.com",
		"insert 54 user54 person54@example.com",
		"insert 77 user77 person77@example.com",
		"insert 7 user7 person7@example.com",
		"insert 25 user25 person25@example.com",
		"insert 71 user71 person71@example.com",
		"insert 13 user13 person13@example.com",
		"insert 22 user22 person22@example.com",
		"insert 53 user53 person53@example.com",
		"insert 51 user51 person51@example.com",
		"insert 59 user59 person59@example.com",
		"insert 32 user32 person32@example.com",
		"insert 36 user36 person36@example.com",
		"insert 79 user79 person79@example.com",
		"insert 10 user10 person10@example.com",
		"insert 33 user33 person33@example.com",
		"insert 20 user20 person20@example.com",
		"insert 4 user4 person4@example.com",
		"insert 35 user35 person35@example.com",
		"insert 76 user76 person76@example.com",
		"insert 49 user49 person49@example.com",
		"insert 24 user24 person24@example.com",
		"insert 70 user70 person70@example.com",
		"insert 48 user48 person48@example.com",
		"insert 39 user39 person39@example.com",
		"insert 15 user15 person15@example.com",
		"insert 47 user47 person47@example.com",
		"insert 30 user30 person30@example.com",
		"insert 86 user86 person86@example.com",
		"insert 31 user31 person31@example.com",
		"insert 68 user68 person68@example.com",
		"insert 37 user37 person37@example.com",
		"insert 66 user66 person66@example.com",
		"insert 63 user63 person63@example.com",
		"insert 40 user40 person40@example.com",
		"insert 78 user78 person78@example.com",
		"insert 19 user19 person19@example.com",
		"insert 46 user46 person46@example.com",
		"insert 14 user14 person14@example.com",
		"insert 81 user81 person81@example.com",
		"insert 72 user72 person72@example.com",
		"insert 6 user6 person6@example.com",
		"insert 50 user50 person50@example.com",
		"insert 85 user85 person85@example.com",
		"insert 67 user67 person67@example.com",
		"insert 2 user2 person2@example.com",
		"insert 55 user55 person55@example.com",
		"insert 69 user69 person69@example.com",
		"insert 5 user5 person5@example.com",
		"insert 65 user65 person65@example.com",
		"insert 52 user52 person52@example.com",
		"insert 1 user1 person1@example.com",
		"insert 29 user29 person29@example.com",
		"insert 9 user9 person9@example.com",
		"insert 43 user43 person43@example.com",
		"insert 75 user75 person75@example.com",
		"insert 21 user21 person21@example.com",
		"insert 82 user82 person82@example.com",
		"insert 12 user12 person12@example.com",
		"insert 18 user18 person18@example.com",
		"insert 60 user60 person60@example.com",
		"insert 44 user44 person44@example.com",
	}

	for _, cmd := range commands {
		stmt, _ := cli.PrepareStatement(cmd)
		executeInsert(stmt, table)
	}

	// Expect no crash.
}

func TestInsertMaxSize(t *testing.T) {
	dbName := "test.db"
	os.Remove(dbName)
	table := dbOpen(dbName)

	// Fill up page, next insert should trigger split.
	for i := 0; i < 384; i++ {
		stmt, _ := cli.PrepareStatement(fmt.Sprintf("insert %d user%d user%d@example.com", i, i, i))
		executeInsert(stmt, table)
	}
	displayTree(table.pager, 0, 0)
	// Expect no crash.
}
