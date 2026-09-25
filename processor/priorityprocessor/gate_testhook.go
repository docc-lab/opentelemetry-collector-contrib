// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import "google.golang.org/grpc/codes"

// SetGateForTesting publishes a gate state without running a controller (tests of the
// priorityotlp receiver). state: "none", "shedlp", "soft"; any other value deactivates the gate.
func SetGateForTesting(state string, lpCode codes.Code) {
	switch state {
	case "none":
		gate.publish(stateNoPressure, lpCode)
	case "shedlp":
		gate.publish(stateShedLP, lpCode)
	case "soft":
		gate.publish(stateSoft, lpCode)
	default:
		gate.active.Store(false)
	}
}
