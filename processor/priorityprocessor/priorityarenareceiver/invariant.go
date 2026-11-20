// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityarenareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor/priorityarenareceiver"

import "fmt"

// broken stops the process on an internal accounting error. The store's bookkeeping is the only
// record of which arena blocks hold which request: carrying on past an inconsistency would send
// the wrong bytes or lose blocks for good. Input cannot cause one; requests are checked before
// they are admitted.
func broken(format string, args ...any) {
	panic(fmt.Sprintf("priorityarena: invariant broken: "+format, args...))
}
