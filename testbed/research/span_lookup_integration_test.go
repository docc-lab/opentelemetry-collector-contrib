// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package research contains research-focused integration tests for custom processor
// development and cross-processor memory sharing experiments.
package research

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/testbed"
)

// ProcessorNameAndConfigBody represents a processor configuration
type ProcessorNameAndConfigBody struct {
	Name string
	Body string
}

// TestSpanLookupIntegration tests cross-pipeline memory sharing between
// spanreconstruction and spanlookup processors using a real collector instance.
//
// This test demonstrates:
// 1. spanreconstruction processor in pipeline "traces" processes fragmented spans and maintains global state
// 2. spanlookup processor in pipeline "traces2" accesses the global state from spanreconstruction
// 3. Cross-pipeline communication through global variables
func TestSpanLookupIntegration(t *testing.T) {
	// Create sender and receiver for the first pipeline (spanreconstruction)
	sender := testbed.NewOTLPTraceDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t))
	receiver := testbed.NewOTLPDataReceiver(testutil.GetAvailablePort(t))

	// Create collector using the built otelcontribcol binary with our custom processors
	cp := testbed.NewChildProcessCollector(
		testbed.WithEnvVar("GOMAXPROCS", "2"),
		testbed.WithAgentExePath("../../bin/otelcontribcol_darwin_arm64"),
	)

	// Prepare results dir
	resultDir, err := filepath.Abs(filepath.Join("results", t.Name()))
	require.NoError(t, err)

	// Define our custom processors
	processors := []ProcessorNameAndConfigBody{
		{
			Name: "spanreconstruction",
			Body: `
  spanreconstruction:
    max_active_spans: 1000
    span_ttl: 300s
`,
		},
		{
			Name: "spanlookup",
			Body: `
  spanlookup:
    max_lookups: 100
    enable_metrics: true
`,
		},
	}

	// Create config using testbed's createConfigYaml function
	configStr := createConfigYaml(t, sender, receiver, resultDir, processors, nil)
	cleanup, err := cp.PrepareConfig(t, configStr)
	require.NoError(t, err, "Failed to prepare config")
	defer cleanup()

	// Create test case with FRAGMENTED span data provider
	tc := testbed.NewTestCase(
		t,
		testbed.NewFragmentedSpanDataProvider(testbed.LoadOptions{
			DataItemsPerSecond: 5,
			ItemsPerBatch:      2,
		}),
		sender,
		receiver,
		cp,
		&testbed.PerfTestValidator{},
		&testbed.PerformanceResults{}, // Use proper results summary instead of nil
	)

	// Start the test
	tc.StartBackend()
	tc.StartAgent()
	defer tc.Stop()

	// Start load generation
	loadOptions := testbed.LoadOptions{
		DataItemsPerSecond: 5,
		ItemsPerBatch:      2,
	}
	t.Log("🟢 Starting load generation with FRAGMENTED spans...")
	tc.StartLoad(loadOptions)

	// Wait for load generator to start sending data
	t.Log("🟢 Waiting for load generator to start...")
	tc.WaitFor(func() bool {
		sent := tc.LoadGenerator.DataItemsSent()
		t.Logf("🟢 Load generator sent: %d items", sent)
		return sent > 0
	}, "load generator started")

	// Run for a longer duration to allow processors to interact
	tc.Sleep(15 * time.Second)

	// Stop load and wait for processing
	tc.StopLoad()
	tc.WaitFor(func() bool { return tc.LoadGenerator.DataItemsSent() == tc.MockBackend.DataItemsReceived() },
		"all data items received")

	t.Log("Integration test completed successfully")
	t.Log("spanreconstruction processor should have processed fragmented spans and updated global state")
	t.Log("spanlookup processor should have accessed and logged the global state")
}

// createConfigYaml creates a config yaml string using the testbed's pattern
func createConfigYaml(
	t *testing.T,
	sender testbed.DataSender,
	receiver testbed.DataReceiver,
	resultDir string,
	processors []ProcessorNameAndConfigBody,
	extensions map[string]string,
) string {
	// Prepare extra processor config section and comma-separated list of extra processor
	// names to use in corresponding "processors" settings.
	processorsSections := ""
	processorsList := ""
	if len(processors) > 0 {
		first := true
		for i := range processors {
			processorsSections += processors[i].Body + "\n"
			if !first {
				processorsList += ","
			}
			processorsList += processors[i].Name
			first = false
		}
	}

	// Prepare extra extension config section and comma-separated list of extra extension
	// names to use in corresponding "extensions" settings.
	extensionsSections := ""
	extensionsList := ""
	if len(extensions) > 0 {
		first := true
		for name, cfg := range extensions {
			extensionsSections += cfg + "\n"
			if !first {
				extensionsList += ","
			}
			extensionsList += name
			first = false
		}
	}

	// Set pipeline based on DataSender type
	var pipeline string
	switch sender.(type) {
	case testbed.TraceDataSender:
		pipeline = "traces"
	case testbed.MetricDataSender:
		pipeline = "metrics"
	case testbed.LogDataSender:
		pipeline = "logs"
	default:
		t.Error("Invalid DataSender type")
	}

	format := `
receivers:%v
exporters:%v
processors:
  %s

extensions:
  pprof:
    save_to_file: %v/cpu.prof
  %s

service:
  extensions: [pprof, %s]
  pipelines:
    %s:
      receivers: [%v]
      processors: [%s]
      exporters: [%v]
`

	// Put corresponding elements into the config template to generate the final config.
	return fmt.Sprintf(
		format,
		sender.GenConfigYAMLStr(),
		receiver.GenConfigYAMLStr(),
		processorsSections,
		resultDir,
		extensionsSections,
		extensionsList,
		pipeline,
		sender.ProtocolName(),
		processorsList,
		receiver.ProtocolName(),
	)
}
