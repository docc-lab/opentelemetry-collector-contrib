// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ipdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/ipdiscoveryreceiver"

import (
	"errors"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

var errMissingEndpointFromConfig = errors.New("missing receiver server endpoint from config")

// Config defines configuration for the IP Discovery receiver.
type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct
}

func (cfg *Config) Validate() error {
	var errs error

	if cfg.Endpoint == "" {
		errs = multierr.Append(errs, errMissingEndpointFromConfig)
	}

	return errs
}
