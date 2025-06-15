package bytes_diff_visualizer

import (
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	fmt.Println("Setting up tests...")
	// Enable color output for tests
	// https://github.com/golang/go/issues/18153#issuecomment-264388969
	// CL:https://go-review.googlesource.com/c/go/+/33857/2/doc/go1.8.html
	color.NoColor = false

	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestVisualizeByteDiffWorksWithStrings(t *testing.T) {
	actual := []byte("Hello, World!")
	expected := []byte("Hello, Go!")

	result := VisualizeByteDiff(actual, expected)
	if len(result) == 0 {
		t.Errorf("Expected a non-empty result")
	}

	expectedLines := []string{
		"Expected (bytes 0-13), hexadecimal:                         | ASCII:",
		"48 65 6c 6c 6f 2c 20 47 6f 21                               | Hello, Go!",
		"",
		"Actual (bytes 0-13), hexadecimal:                           | ASCII:",
		"48 65 6c 6c 6f 2c 20 57 6f 72 6c 64 21                      | Hello, World!",
	}

	for i, expectedLine := range expectedLines {
		actualLine := stripANSI(result[i])
		if i >= len(result) {
			t.Fatalf("Expected %v lines, but only got %v", len(expectedLines), len(result))
		}

		if !assert.Equal(t, expectedLine, actualLine) {
			t.FailNow()
		}
	}

	assert.Equal(t, len(expectedLines), len(result))
}

func TestVisualizeByteDiffWorksWithNonPrintableCharacters(t *testing.T) {
	actual := []byte("blob\000header")
	expected := []byte("blob\000\000header") // Has an extra null byte

	result := VisualizeByteDiff(actual, expected)

	expectedLines := []string{
		"Expected (bytes 0-12), hexadecimal:                         | ASCII:",
		"62 6c 6f 62 00 00 68 65 61 64 65 72                         | blob..header",
		"",
		"Actual (bytes 0-12), hexadecimal:                           | ASCII:",
		"62 6c 6f 62 00 68 65 61 64 65 72                            | blob.header",
	}

	for i, expectedLine := range expectedLines {
		actualLine := stripANSI(result[i])
		if i >= len(result) {
			t.Fatalf("Expected %v lines, but only got %v", len(expectedLines), len(result))
		}

		if !assert.Equal(t, expectedLine, actualLine) {
			t.FailNow()
		}
	}

	assert.Equal(t, len(expectedLines), len(result))
}

func TestVisualizeByteDiffWorksWithLongerSequences(t *testing.T) {
	expected := []byte("1234567890123457890123457890abcd")
	actual := []byte("1234567890123457890123457890efgh")

	result := VisualizeByteDiff(actual, expected)

	if len(result) == 0 {
		t.Errorf("Expected a non-empty result")
	}

	expectedLines := []string{
		"Expected (bytes 0-32), hexadecimal:                         | ASCII:",
		"31 32 33 34 35 36 37 38 39 30 31 32 33 34 35 37 38 39 30 31 | 12345678901234578901",
		"32 33 34 35 37 38 39 30 61 62 63 64                         | 23457890abcd",
		"",
		"Actual (bytes 0-32), hexadecimal:                           | ASCII:",
		"31 32 33 34 35 36 37 38 39 30 31 32 33 34 35 37 38 39 30 31 | 12345678901234578901",
		"32 33 34 35 37 38 39 30 65 66 67 68                         | 23457890efgh",
	}

	for i, expectedLine := range expectedLines {
		actualLine := stripANSI(result[i])
		if i >= len(result) {
			t.Fatalf("Expected %v lines, but only got %v", len(expectedLines), len(result))
		}

		if !assert.Equal(t, expectedLine, actualLine) {
			t.FailNow()
		}
	}

	assert.Equal(t, len(expectedLines), len(result))
}

func TestVisualizeByteDiffWorksWithColoredOutput(t *testing.T) {
	actual := []byte("Hello, World!")
	expected := []byte("Hello, Go!")

	result := VisualizeByteDiff(actual, expected)
	if len(result) == 0 {
		t.Errorf("Expected a non-empty result")
	}

	expectedLines := []string{
		"Expected (bytes 0-13), hexadecimal:                         | ASCII:",
		"48 65 6c 6c 6f 2c 20 " + colorizeString(color.FgHiGreen, "47") + " 6f 21                               | Hello, " + colorizeString(color.FgHiGreen, "G") + "o!",
		"",
		"Actual (bytes 0-13), hexadecimal:                           | ASCII:",
		"48 65 6c 6c 6f 2c 20 " + colorizeString(color.FgHiRed, "57") + " 6f 72 6c 64 21                      | Hello, " + colorizeString(color.FgHiRed, "W") + "orld!",
	}

	for i, expectedLine := range expectedLines {
		actualLine := result[i]
		if i >= len(result) {
			t.Fatalf("Expected %v lines, but only got %v", len(expectedLines), len(result))
		}

		if !assert.Equal(t, expectedLine, actualLine) {
			t.FailNow()
		}
	}

	assert.Equal(t, len(expectedLines), len(result))
}

func stripANSI(data string) string {
	// https://github.com/acarl005/stripansi/blob/master/stripansi.go
	const ansi = "[\u001B\u009B][[\\]()#;?]*(?:(?:(?:[a-zA-Z\\d]*(?:;[a-zA-Z\\d]*)*)?\u0007)|(?:(?:\\d{1,4}(?:;\\d{0,4})*)?[\\dA-PRZcf-ntqry=><~]))"

	var re = regexp.MustCompile(ansi)

	return string(re.ReplaceAll([]byte(data), []byte("")))
}
