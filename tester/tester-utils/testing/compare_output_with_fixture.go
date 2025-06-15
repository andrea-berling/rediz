package testing

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/codecrafters-io/tester-utils/executable"
)

func CompareOutputWithFixture(t *testing.T, testerOutput []byte, normalizeOutputFunc func([]byte) []byte, fixturePath string) {
	shouldRecordFixture := os.Getenv("CODECRAFTERS_RECORD_FIXTURES") == "true"

	fixtureContents, err := os.ReadFile(fixturePath)
	if err != nil {
		if os.IsNotExist(err) {
			if shouldRecordFixture {
				writeOrOverwriteFixture(fixturePath, testerOutput)
				return
			} else {
				t.Errorf("Fixture file %s does not exist. To create a new one, use CODECRAFTERS_RECORD_FIXTURES=true", fixturePath)
				return
			}
		}

		panic(err)
	}

	normalizedTesterOutput := normalizeOutputFunc(testerOutput)
	normalizedFixturesContents := normalizeOutputFunc(fixtureContents)

	if bytes.Equal(normalizedTesterOutput, normalizedFixturesContents) {
		return
	}

	if shouldRecordFixture {
		writeOrOverwriteFixture(fixturePath, testerOutput)
		return
	}

	diffExecutablePath, err := exec.LookPath("diff")
	if err != nil {
		panic(err)
	}

	diffExecutable := executable.NewExecutable(diffExecutablePath)

	testerOutputTmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}

	if _, err = testerOutputTmpFile.Write(normalizedTesterOutput); err != nil {
		panic(err)
	}

	fixtureTmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}

	if _, err = fixtureTmpFile.Write(normalizedFixturesContents); err != nil {
		panic(err)
	}

	result, err := diffExecutable.Run("-u", fixtureTmpFile.Name(), testerOutputTmpFile.Name())
	if err != nil {
		panic(err)
	}

	// Remove the first two lines of the diff output
	diffContents := bytes.SplitN(result.Stdout, []byte("\n"), 3)[2]

	os.Stdout.Write([]byte("\n\nDifferences detected:\n\n"))
	os.Stdout.Write(diffContents)
	os.Stdout.Write([]byte("\n\nRe-run this test with CODECRAFTERS_RECORD_FIXTURES=true to update fixtures\n\n"))
	t.FailNow()
}

func writeOrOverwriteFixture(fixturePath string, testerOutput []byte) {
	if err := os.MkdirAll(filepath.Dir(fixturePath), os.ModePerm); err != nil {
		panic(err)
	}

	if err := os.WriteFile(fixturePath, testerOutput, 0644); err != nil {
		panic(err)
	}
}
