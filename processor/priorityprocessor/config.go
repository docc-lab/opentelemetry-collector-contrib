// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package priorityprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/priorityprocessor"

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

var ErrInvalidConfigValue = errors.New("invalid config value")

var _ component.Config = (*Config)(nil)

// Config defines the configuration for the processor.
type Config struct {
	// TODO: Add configuration fields as needed
}

// Validate checks whether the input configuration has all of the required fields for the processor.
// An error is returned if there are any invalid inputs.
func (config *Config) Validate() error {
	// TODO: Add validation logic
	return nil
}
