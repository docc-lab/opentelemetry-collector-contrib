// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/configdiscoveryreceiver"

import (
	"errors"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

var errMissingEndpointFromConfig = errors.New("missing receiver server endpoint from config")

// Config defines configuration for the Config Discovery receiver.
type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct

	// ConfigMap stores the configuration properties that can be discovered.
	// Values can be nested maps or arrays, which will be parsed from YAML
	// and stored as map[string]interface{} and []interface{} (JSON-serializable).
	ConfigMap map[string]interface{} `mapstructure:"config_map"`
}

func (cfg *Config) Validate() error {
	var errs error

	if cfg.Endpoint == "" {
		errs = multierr.Append(errs, errMissingEndpointFromConfig)
	}

	return errs
}
