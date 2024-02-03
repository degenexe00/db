package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const cliName string = "simpleRELP"
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
const rowsPerPage = pageSize / rowSize
const tableMaxRows = rowsPerPage * tableMaxPages

type Page [pageSize]byte

type Table struct {
	numRows uint32
	pages   []*Page
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

func rowSlot(t *Table, rowNum uint32) ([]byte, error) {
	pageNum := rowNum / rowsPerPage
	if pageNum > tableMaxPages {
		return []byte{}, fmt.Errorf("received %d but expected page number below %d", pageNum, tableMaxPages)
	}

	if t.pages[pageNum] == nil {
		t.pages[pageNum] = new(Page)
	}

	rowOffset := rowNum % rowsPerPage
	byteOffset := rowOffset * rowSize
	return t.pages[pageNum][byteOffset : byteOffset+rowSize], nil
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
	if table.numRows >= tableMaxPages {
		return fmt.Errorf("table full")
	}
	rawRow, err := rowSlot(table, table.numRows)
	if err != nil {
		return err
	}
	n := copy(rawRow, serializeRow(&stmt.rowToInsert))
	if n != int(rowSize) {
		return fmt.Errorf("copied only %d elements, but expected to copy %d elements", n, rowSize)
	}
	table.numRows++
	return nil
}

func executeSelect(stmt *Statement, table *Table) error {
	for i := 0; i < int(table.numRows); i++ {
		rawRow, err := rowSlot(table, uint32(i))
		if err != nil {
			return err
		}
		row := deserializeRow(rawRow)
		printRow(row)
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

func newTable() *Table {
	return &Table{
		numRows: 0,
		pages:   make([]*Page, tableMaxPages),
	}
}

func main() {
	table := newTable()
	reader := bufio.NewScanner(os.Stdin)
	commands := map[string]interface{}{
		".help":  displayHelp,
		".clear": clearScreen,
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
