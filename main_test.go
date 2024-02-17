package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

const dbFile = "test.db"

func TestMain(m *testing.M) {
	// Compile the binary
	cmd := exec.Command("go", "build")
	if err := cmd.Run(); err != nil {
		// Handle the error, possibly fail the test
		os.Exit(1)
	}

	// Run the tests
	code := m.Run()

	// Exit with the status code of the test run
	os.Exit(code)
}

func TestPrintStructureOfThreeLeafNode(t *testing.T) {
	deleteDb()
	inputs := []string{}
	for i := 1; i <= 14; i++ {
		inputs = append(inputs, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	inputs = append(inputs, ".btree")
	inputs = append(inputs, "insert 15 user15 person15@example.com")
	inputs = append(inputs, ".exit")
	expectedOutputs := []string{
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Tree:",
		"- internal (size 1)",
		"  - leaf (size 7)",
		"    - 1",
		"    - 2",
		"    - 3",
		"    - 4",
		"    - 5",
		"    - 6",
		"    - 7",
		"  - key 7",
		"  - leaf (size 7)",
		"    - 8",
		"    - 9",
		"    - 10",
		"    - 11",
		"    - 12",
		"    - 13",
		"    - 14",
		"simpleDB> Executed.",
		"simpleDB> ",
	}
	output := dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
}

func TestPrintStructureOfFourLeafNode(t *testing.T) {
	deleteDb()
	inputs := []string{
		"insert 18 user18 person18@example.com",
		"insert 7 user7 person7@example.com",
		"insert 10 user10 person10@example.com",
		"insert 29 user29 person29@example.com",
		"insert 23 user23 person23@example.com",
		"insert 4 user4 person4@example.com",
		"insert 14 user14 person14@example.com",
		"insert 30 user30 person30@example.com",
		"insert 15 user15 person15@example.com",
		"insert 26 user26 person26@example.com",
		"insert 22 user22 person22@example.com",
		"insert 19 user19 person19@example.com",
		"insert 2 user2 person2@example.com",
		"insert 1 user1 person1@example.com",
		"insert 21 user21 person21@example.com",
		"insert 11 user11 person11@example.com",
		"insert 6 user6 person6@example.com",
		"insert 20 user20 person20@example.com",
		"insert 5 user5 person5@example.com",
		"insert 8 user8 person8@example.com",
		"insert 9 user9 person9@example.com",
		"insert 3 user3 person3@example.com",
		"insert 12 user12 person12@example.com",
		"insert 27 user27 person27@example.com",
		"insert 17 user17 person17@example.com",
		"insert 16 user16 person16@example.com",
		"insert 13 user13 person13@example.com",
		"insert 24 user24 person24@example.com",
		"insert 25 user25 person25@example.com",
		"insert 28 user28 person28@example.com",
		".btree",
		".exit",
	}
	expectedOutputs := []string{
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Tree:",
		"- internal (size 3)",
		"  - leaf (size 7)",
		"    - 1",
		"    - 2",
		"    - 3",
		"    - 4",
		"    - 5",
		"    - 6",
		"    - 7",
		"  - key 7",
		"  - leaf (size 8)",
		"    - 8",
		"    - 9",
		"    - 10",
		"    - 11",
		"    - 12",
		"    - 13",
		"    - 14",
		"    - 15",
		"  - key 15",
		"  - leaf (size 7)",
		"    - 16",
		"    - 17",
		"    - 18",
		"    - 19",
		"    - 20",
		"    - 21",
		"    - 22",
		"  - key 22",
		"  - leaf (size 8)",
		"    - 23",
		"    - 24",
		"    - 25",
		"    - 26",
		"    - 27",
		"    - 28",
		"    - 29",
		"    - 30",
		"simpleDB> ",
	}
	output := dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
}

func TestSelectAllMultiLevel(t *testing.T) {
	deleteDb()
	inputs := []string{}
	for i := 1; i <= 15; i++ {
		inputs = append(inputs, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	inputs = append(inputs, "select")
	inputs = append(inputs, ".exit")
	expectedOutputs := []string{
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> (1, user1, person1@example.com)",
		"(2, user2, person2@example.com)",
		"(3, user3, person3@example.com)",
		"(4, user4, person4@example.com)",
		"(5, user5, person5@example.com)",
		"(6, user6, person6@example.com)",
		"(7, user7, person7@example.com)",
		"(8, user8, person8@example.com)",
		"(9, user9, person9@example.com)",
		"(10, user10, person10@example.com)",
		"(11, user11, person11@example.com)",
		"(12, user12, person12@example.com)",
		"(13, user13, person13@example.com)",
		"(14, user14, person14@example.com)",
		"(15, user15, person15@example.com)",
		"Executed.",
		"simpleDB> ",
	}
	output := dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
}

func TestPrintStructureOneNodeTree(t *testing.T) {
	deleteDb()
	inputs := []string{}
	for i := 1; i <= 3; i++ {
		inputs = append(inputs, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	inputs = append(inputs, ".btree")
	inputs = append(inputs, ".exit")
	expectedOutputs := []string{
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Executed.",
		"simpleDB> Tree:",
		"- leaf (size 3)",
		"  - 1",
		"  - 2",
		"  - 3",
		"simpleDB> ",
	}
	output := dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
}

func TestInsertAndSelect(t *testing.T) {
	deleteDb()
	inputs := []string{
		"insert 1 michal foo@bar.com",
		"select",
		".exit",
	}
	expectedOutputs := []string{
		"simpleDB> Executed.",
		"simpleDB> (1, michal, foo@bar.com)",
		"Executed.",
		"simpleDB> ",
	}
	output := dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
}

func TestTableIsFull(t *testing.T) {
	deleteDb()
	inputs := []string{}
	for i := 1; i < 1400; i++ {
		inputs = append(inputs, fmt.Sprintf("insert %d user%d person%d@example.com", i, i, i))
	}
	inputs = append(inputs, ".exit")
	output := dbDriver(t, inputs)
	lines := strings.Split(output.String(), "\n")
	if len(lines) < 2 {
		t.Fatal("Expected more than 2 lines")
	}
	if lines[len(lines)-2] != "simpleDB> Error: table full" {
		t.Fatalf("Expected table full error, got: %v\n", lines[len(lines)-2])
	}
}

func TestMaximumLengthStrings(t *testing.T) {
	deleteDb()
	longUsername := strings.Repeat("a", 32)
	longEmail := strings.Repeat("a", 255)
	inputs := []string{
		fmt.Sprintf("insert 1 %s %s", longUsername, longEmail),
		"select",
		".exit",
	}
	output := dbDriver(t, inputs)
	expected := []string{
		"simpleDB> Executed.",
		fmt.Sprintf("simpleDB> (1, %s, %s)", longUsername, longEmail),
		"Executed.",
		"simpleDB> ",
	}
	assertEqual(output, expected, t)
}

func TestErrorIfStringTooLong(t *testing.T) {
	deleteDb()
	longUsername := strings.Repeat("a", 33)
	longEmail := strings.Repeat("a", 256)
	inputs := []string{
		fmt.Sprintf("insert 1 %s %s", longUsername, longEmail),
		"select",
		".exit",
	}
	output := dbDriver(t, inputs)
	expected := []string{
		"simpleDB> Error: string is too long.",
		"simpleDB> Executed.",
		"simpleDB> ",
	}
	assertEqual(output, expected, t)
}

func TestPersistData(t *testing.T) {
	deleteDb()
	inputs := []string{
		"insert 1 michal foo@bar.com",
		".exit",
	}
	expectedOutputs := []string{
		"simpleDB> Executed.",
		"simpleDB> ",
	}
	output := dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
	inputs = []string{
		"select",
		".exit",
	}
	expectedOutputs = []string{
		"simpleDB> (1, michal, foo@bar.com)",
		"Executed.",
		"simpleDB> ",
	}
	output = dbDriver(t, inputs)
	assertEqual(output, expectedOutputs, t)
}

func dbDriver(t *testing.T, inputs []string) bytes.Buffer {
	cmd := exec.Command("./db_from_scratch", dbFile)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("Failed to open stdin: %v", err)
	}
	defer stdin.Close()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start command: %v", err)
	}

	// Send a command to the CLI
	err = sendCommands(stdin, inputs)
	if err != nil {
		t.Fatalf("Failed to send commands: %v", err)
	}
	stdin.Close()

	// Wait for the command to complete
	err = cmd.Wait()
	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}

	return stdout
}

func assertEqual(stdout bytes.Buffer, expected []string, t *testing.T) {
	scanner := bufio.NewScanner(&stdout)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line != expected[i] {
			t.Fatalf("Unexpected output on line %d,\ngot: '%s'\nexpected: '%s'\n", i, line, expected[i])
		}
		i++
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}
}

func sendCommands(stdin io.WriteCloser, commands []string) error {
	for _, cmd := range commands {
		_, err := stdin.Write([]byte(fmt.Sprintf("%s\n", cmd)))
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteDb() {
	os.Remove("test.db")
}
