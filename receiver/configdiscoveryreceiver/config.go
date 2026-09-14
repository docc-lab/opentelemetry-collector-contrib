// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/configdiscoveryreceiver"

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"

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
	// Tomislav-RetCtx: cpd_min/cpd_max configure forward and reverse SDK TTL draws.
	// Both are required, with 1 <= cpd_min <= cpd_max <= 256. Without them,
	// cpd retains its existing fixed-distance meaning.
	// Tomislav-RetCtx: reverse_policy selects ttl (default), probability,
	// inverse_depth, or depth_linear. Only probability takes reverse_probability.
	ConfigMap map[string]interface{} `mapstructure:"config_map"`
}

func (cfg *Config) Validate() error {
	var errs error

	if cfg.Endpoint == "" {
		errs = multierr.Append(errs, errMissingEndpointFromConfig)
	}
	if err := validateCheckpointRange(cfg.ConfigMap); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := validateReversePolicy(cfg.ConfigMap); err != nil {
		errs = multierr.Append(errs, err)
	}

	return errs
}

// Tomislav-RetCtx: fail invalid or partial ranges before serving them to SDKs.
func validateCheckpointRange(config map[string]interface{}) error {
	low, hasLow := config["cpd_min"]
	high, hasHigh := config["cpd_max"]
	if !hasLow && !hasHigh {
		return nil
	}
	minCPD, okLow := checkpointBound(low)
	maxCPD, okHigh := checkpointBound(high)
	if !hasLow || !hasHigh || !okLow || !okHigh || minCPD > maxCPD {
		return errors.New("config_map: require integer 1 <= cpd_min <= cpd_max <= 256")
	}
	return nil
}

func checkpointBound(value interface{}) (int, bool) {
	var n int64
	switch v := value.(type) {
	case int:
		n = int64(v)
	case int64:
		n = v
	case float64:
		if math.IsNaN(v) || v < 1 || v > 256 || math.Trunc(v) != v {
			return 0, false
		}
		n = int64(v)
	case json.Number:
		var err error
		n, err = v.Int64()
		if err != nil {
			return 0, false
		}
	case string:
		var err error
		n, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	return int(n), n >= 1 && n <= 256
}
