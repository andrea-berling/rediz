package tester_definition

import (
	"time"

	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

// TestCase represents a test case that'll be run against the user's code.
//
// For now, we only support one test case per stage. This may change in the future.
//
// We enforce the one-test-case-per-stage rule by requiring that the test case's slug matches the stage's slug (from the YAML definition).
type TestCase struct {
	// Slug is the unique identifier for this test case. For now, it must match the slug of the stage from the course's YAML definition.
	Slug string

	// TestFunc is the function that'll be run against the user's code.
	TestFunc func(testCaseHarness *test_case_harness.TestCaseHarness) error

	// Timeout is the maximum amount of time that the test case can run for.
	Timeout time.Duration
}

func (t TestCase) CustomOrDefaultTimeout() time.Duration {
	if (t.Timeout == 0) || (t.Timeout == time.Duration(0)) {
		return 10 * time.Second
	} else {
		return t.Timeout
	}
}

type TesterDefinition struct {
	// Example: spawn_redis_server.sh
	ExecutableFileName       string
	LegacyExecutableFileName string

	TestCases          []TestCase
	AntiCheatTestCases []TestCase
}

func (t TesterDefinition) TestCaseBySlug(slug string) TestCase {
	for _, testCase := range t.TestCases {
		if testCase.Slug == slug {
			return testCase
		}
	}

	return TestCase{}
}
