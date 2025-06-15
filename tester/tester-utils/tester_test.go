package tester_utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/codecrafters-io/tester-utils/test_case_harness"
	"github.com/codecrafters-io/tester-utils/tester_definition"
	"github.com/stretchr/testify/assert"
)

func passFunc(harness *test_case_harness.TestCaseHarness) error {
	return nil
}

func failFunc(harness *test_case_harness.TestCaseHarness) error {
	return errors.New("fail")
}

func buildTestCasesJson(slugs []string) string {
	testCases := []map[string]string{}

	for index, slug := range slugs {
		testCases = append(testCases, map[string]string{
			"slug":              slug,
			"tester_log_prefix": fmt.Sprintf("test-%d", index+1),
			"title":             fmt.Sprintf("Stage #%d: %s", index+1, slug),
		})
	}

	testCasesJson, _ := json.Marshal(testCases)
	return string(testCasesJson)
}

func TestAllStagesPass(t *testing.T) {
	definition := tester_definition.TesterDefinition{
		TestCases: []tester_definition.TestCase{
			{Slug: "test-1", TestFunc: passFunc},
			{Slug: "test-2", TestFunc: passFunc},
		},
	}
	env := map[string]string{
		"CODECRAFTERS_REPOSITORY_DIR":  "./test_helpers/valid_app_dir",
		"CODECRAFTERS_TEST_CASES_JSON": buildTestCasesJson([]string{"test-1", "test-2"}),
	}
	exitCode := RunCLI(env, definition)
	assert.Equal(t, exitCode, 0)
}

func TestOneStageFails(t *testing.T) {
	definition := tester_definition.TesterDefinition{
		TestCases: []tester_definition.TestCase{
			{Slug: "test-1", TestFunc: passFunc},
			{Slug: "test-2", TestFunc: failFunc},
		},
	}

	env := map[string]string{
		"CODECRAFTERS_REPOSITORY_DIR":  "./test_helpers/valid_app_dir",
		"CODECRAFTERS_TEST_CASES_JSON": buildTestCasesJson([]string{"test-1", "test-2"}),
	}
	exitCode := RunCLI(env, definition)
	assert.Equal(t, exitCode, 1)
}
