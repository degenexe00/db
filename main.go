package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const cliName string = "simpleRELP"
const dbName string = "simpleDB"

type statement struct {
	stmtType statementType
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

func prepareStatement(text string) (*statement, error) {
	if strings.EqualFold(text, "insert") {
		return &statement{stmtType: stmtInsert}, nil
	}
	if strings.EqualFold(text, "select") {
		return &statement{stmtType: stmtSelect}, nil
	}
	return nil, fmt.Errorf("unknown statement: %v", text)
}

func executeStatement(stmt *statement) {
	switch stmt.stmtType {
	case stmtInsert:
		fmt.Println("TODO: handle insert")
	case stmtSelect:
		fmt.Println("TODO: handle select")
	}
}

func main() {
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
				fmt.Printf("Unrecognized command: %v\n", text)
				continue
			}
			executeStatement(stmt)
		}
	}
}
