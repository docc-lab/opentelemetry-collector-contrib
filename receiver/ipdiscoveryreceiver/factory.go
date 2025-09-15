// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ipdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/ipdiscoveryreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/ipdiscoveryreceiver/internal/metadata"
)

// NewFactory creates a factory for the IP Discovery receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

// createDefaultConfig creates the default configuration for the IP Discovery receiver.
func createDefaultConfig() component.Config {
	return &Config{}
}

// createLogsReceiver creates a logs receiver based on provided config.
func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	_ consumer.Logs,
) (receiver.Logs, error) {
	conf := cfg.(*Config)
	return newIPDiscoveryReceiver(params, *conf)
}
