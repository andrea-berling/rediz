package bytes_diff_visualizer

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/fatih/color"
)

// VisualizeByteDiff visualizes the difference between two byte slices, returning lines to be presented to the user.
//
// The lines will include ANSI escape codes to colorize the output.
func VisualizeByteDiff(actual []byte, expected []byte) []string {
	// If both are exactly the same, return an empty slice
	if bytes.Equal(actual, expected) {
		return []string{}
	}

	// Find the index of the first differing byte
	var firstDiffIndex int = -1

	for i := 0; i < len(actual) && i < len(expected); i++ {
		if actual[i] != expected[i] {
			firstDiffIndex = i
			break
		}
	}

	// If we got here, then the first differing byte is the first byte of the longer slice
	if firstDiffIndex == -1 {
		if len(actual) > len(expected) {
			firstDiffIndex = len(expected)
		} else {
			firstDiffIndex = len(actual)
		}
	}

	totalByteCountToDisplay := 100
	byteCountPerLine := 20
	byteRangeStart := intmax(0, firstDiffIndex-(totalByteCountToDisplay/2))
	byteRangeEnd := intmin(byteRangeStart+totalByteCountToDisplay, intmax(len(actual), len(expected)))

	linesBuffer := bytes.NewBuffer([]byte{})

	leftHeader := PadRight(fmt.Sprintf("Expected (bytes %v-%v), hexadecimal:", byteRangeStart, byteRangeEnd), " ", 60)
	fmt.Fprintf(linesBuffer, "%s| ASCII:\n", leftHeader)

	for i := byteRangeStart; i < intmin(byteRangeEnd, len(expected)); i += byteCountPerLine {
		end := intmin(i+byteCountPerLine, len(expected))

		bytesAsHex := formatHexWithColorizedByte(expected, i, firstDiffIndex, end, color.FgHiGreen)
		bytesAsAscii := formatAsciiWithColorizedByte(expected, i, firstDiffIndex, end, color.FgHiGreen)

		fmt.Fprintf(linesBuffer, "%v| %v\n", bytesAsHex, bytesAsAscii)
	}

	linesBuffer.Write([]byte("\n"))

	leftHeader = PadRight(fmt.Sprintf("Actual (bytes %v-%v), hexadecimal:", byteRangeStart, byteRangeEnd), " ", 60)
	fmt.Fprintf(linesBuffer, "%s| ASCII:\n", leftHeader)

	for i := byteRangeStart; i < intmin(byteRangeEnd, len(actual)); i += byteCountPerLine {
		end := intmin(i+byteCountPerLine, len(actual))

		bytesAsHex := formatHexWithColorizedByte(actual, i, firstDiffIndex, end, color.FgHiRed)
		bytesAsAscii := formatAsciiWithColorizedByte(actual, i, firstDiffIndex, end, color.FgHiRed)

		fmt.Fprintf(linesBuffer, "%v| %v\n", bytesAsHex, bytesAsAscii)
	}

	output := linesBuffer.String()
	if output[len(output)-1] == '\n' {
		output = output[:len(output)-1]
	}
	return strings.Split(output, "\n")
}

func formatBytesAsAscii(value []byte) string {
	var asciiRepresentations []string

	for _, b := range value {
		if b < 32 || b > 126 {
			// If the byte is not printable, replace it with a dot
			asciiRepresentations = append(asciiRepresentations, ".")
		} else {
			asciiRepresentations = append(asciiRepresentations, string(b))
		}
	}

	return strings.Join(asciiRepresentations, "")
}

func formatHexWithColorizedByte(value []byte, i int, firstDiffIndex int, end int, chosenColor color.Attribute) string {
	if firstDiffIndex >= i && firstDiffIndex < end {
		return PadRight(formatHexWithColorizedByteHelper(value, i, firstDiffIndex, end, chosenColor), " ", 69)
	} else {
		return PadRight(formatBytesAsHex(value[i:end]), " ", 60)
	}
}

func formatHexWithColorizedByteHelper(value []byte, i int, firstDiffIndex int, end int, chosenColor color.Attribute) string {
	return formatBytesAsHex(value[i:firstDiffIndex]) + " " + colorizeString(chosenColor, formatBytesAsHex(value[firstDiffIndex:firstDiffIndex+1])) + " " + formatBytesAsHex(value[firstDiffIndex+1:end])
}

func formatAsciiWithColorizedByte(value []byte, i int, firstDiffIndex int, end int, chosenColor color.Attribute) string {
	if firstDiffIndex >= i && firstDiffIndex < end {
		return formatAsciiWithColorizedByteHelper(value, i, firstDiffIndex, end, chosenColor)
	} else {
		return formatBytesAsAscii(value[i:end])
	}
}

func formatAsciiWithColorizedByteHelper(value []byte, i int, firstDiffIndex int, end int, chosenColor color.Attribute) string {
	return formatBytesAsAscii(value[i:firstDiffIndex]) + colorizeString(chosenColor, formatBytesAsAscii(value[firstDiffIndex:firstDiffIndex+1])) + formatBytesAsAscii(value[firstDiffIndex+1:end])
}

func formatBytesAsHex(value []byte) string {
	var hexadecimalRepresentations []string

	for _, b := range value {
		hexadecimalRepresentations = append(hexadecimalRepresentations, fmt.Sprintf("%02x", b))
	}

	return strings.Join(hexadecimalRepresentations, " ")
}

func intmax(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func intmin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func colorizeString(colorToUse color.Attribute, msg string) string {
	c := color.New(colorToUse)
	return c.Sprint(msg)
}

func PadRight(str, pad string, length int) string {
	for {
		str += pad
		if len(str) > length {
			return str[0:length]
		}
	}
}
func PadLeft(str, pad string, length int) string {
	for {
		str = pad + str
		if len(str) > length {
			return str[0:length]
		}
	}
}
