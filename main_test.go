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

func TestInsertAndSelect(t *testing.T) {
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
	inputs := []string{}
	for i := 0; i < 1400; i++ {
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
	cmd := exec.Command("./db_from_scratch")
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
			t.Errorf("Unexpected output,\ngot: '%s'\nexpected: '%s'\n", line, expected[i])
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
