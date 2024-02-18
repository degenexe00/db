package cli

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/MichalPitr/db_from_scratch/pkg/constants"
	"github.com/MichalPitr/db_from_scratch/pkg/types"
)

func PrepareStatement(text string) (*types.Statement, error) {
	if strings.EqualFold(text[:6], "insert") {
		stmt := types.Statement{
			StmtType:    types.StmtInsert,
			RowToInsert: types.Row{},
		}
		var username, email string
		n, err := fmt.Sscanf(text, "insert %d %s %s", &stmt.RowToInsert.Id, &username, &email)
		if err != nil {
			return nil, err
		}
		if n < 3 {
			return nil, fmt.Errorf("expected 3 arguments for insert, but got %d", n)
		}

		if len(username) > int(constants.UsernameSize) {
			return nil, fmt.Errorf("string is too long")
		}

		if len(email) > int(constants.EmailSize) {
			return nil, fmt.Errorf("string is too long")
		}

		copy(stmt.RowToInsert.Username[:], []byte(username))
		copy(stmt.RowToInsert.Email[:], []byte(email))
		return &stmt, nil
	}
	if strings.EqualFold(text, "select") {
		return &types.Statement{StmtType: types.StmtSelect}, nil
	}
	return nil, fmt.Errorf("unknown statement: %v", text)
}

func PrintRow(row types.Row) {
	username := string(bytes.Trim(row.Username[:], "\x00"))
	email := string(bytes.Trim(row.Email[:], "\x00"))
	fmt.Printf("(%d, %s, %s)\n", row.Id, username, email)
}

func PrintPrompt() {
	fmt.Printf("%v> ", constants.DbName)
}

func DisplayHelp() {
	fmt.Printf("Welcome to %v! These are the available commands:\n", constants.CliName)
	fmt.Println(".help    - Show available commands")
	fmt.Println(".clear   - Clear the terminal screen")
	fmt.Println(".exit    - Closes your connection to", constants.DbName)
}

func DisplayConstants() {
	fmt.Println("Constants:")
	fmt.Printf("rowSize: %d\n", constants.RowSize)
	fmt.Printf("commonNodeHeaderSize: %d\n", constants.CommonNodeHeaderSize)
	fmt.Printf("leafNodeHeaderSize: %d\n", constants.LeafNodeHeaderSize)
	fmt.Printf("leafNodeCellSize: %d\n", constants.LeafNodeCellSize)
	fmt.Printf("leafNodeSpaceForCells: %d\n", constants.LeafNodeSpaceForcells)
	fmt.Printf("leafNodeMaxCells: %d\n", constants.LeafNodeMaxCells)
}

func ClearScreen() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func CleanInput(text string) string {
	output := strings.TrimSpace(text)
	output = strings.ToLower(output)
	return output
}

func HandleCmd(cmd string) {
	fmt.Printf("Unknown command: %v\n", cmd)
}

func Indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf("  ")
	}
}
