// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package ipdiscoveryreceiver provides an HTTP server that exposes the agent's IP address
// for use by other components that need to know the agent's network identity.
package ipdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/ipdiscoveryreceiver"
