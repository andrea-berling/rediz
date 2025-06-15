package tester_context

import (
	"fmt"
	"testing"

	"github.com/codecrafters-io/tester-utils/tester_definition"
	"github.com/stretchr/testify/assert"
)

func TestRequiresAppDir(t *testing.T) {
	_, err := GetTesterContext(map[string]string{
		"CODECRAFTERS_TEST_CASES_JSON": `[{ "slug": "test", "tester_log_prefix": "test", "title": "Test"}]`,
	}, tester_definition.TesterDefinition{})
	if !assert.Error(t, err) {
		t.FailNow()
	}
}

func TestRequiresCurrentStageSlug(t *testing.T) {
	_, err := GetTesterContext(map[string]string{
		"CODECRAFTERS_REPOSITORY_DIR": "./test_helpers/valid_app_dir",
	}, tester_definition.TesterDefinition{})
	if !assert.Error(t, err) {
		t.FailNow()
	}
}

func TestSuccessParsingTestCases(t *testing.T) {
	context, err := GetTesterContext(map[string]string{
		"CODECRAFTERS_TEST_CASES_JSON": `[{ "slug": "test", "tester_log_prefix": "test", "title": "Test"}]`,
		"CODECRAFTERS_REPOSITORY_DIR":  "./test_helpers/valid_app_dir",
	}, tester_definition.TesterDefinition{})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, len(context.TestCases), 1)
	assert.Equal(t, context.TestCases[0].Slug, "test")
	assert.Equal(t, context.TestCases[0].TesterLogPrefix, "test")
	assert.Equal(t, context.TestCases[0].Title, "Test")
}

func TestCorrectExecutable(t *testing.T) {
	tests := []struct {
		submissionDir      string
		expectedExecutable string
	}{
		{"valid_app_dir", "your_program.sh"}, // neither executables present
		{"valid_app_dir_legacy_only", "spawn_redis_server.sh"},
		{"valid_app_dir_both", "your_program.sh"},
	}

	for _, tt := range tests {
		context, err := GetTesterContext(map[string]string{
			"CODECRAFTERS_TEST_CASES_JSON": `[{ "slug": "test", "tester_log_prefix": "test", "title": "Test"}]`,
			"CODECRAFTERS_REPOSITORY_DIR":  fmt.Sprintf("./test_helpers/%s", tt.submissionDir),
		}, tester_definition.TesterDefinition{
			ExecutableFileName:       "your_program.sh",
			LegacyExecutableFileName: "spawn_redis_server.sh",
		})

		if !assert.NoError(t, err) {
			t.FailNow()
		}

		assert.Equal(t, context.ExecutablePath, fmt.Sprintf("test_helpers/%s/%s", tt.submissionDir, tt.expectedExecutable))
	}
}
