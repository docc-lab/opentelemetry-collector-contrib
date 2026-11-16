// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import "math"

func floatBits(f float64) uint64 { return math.Float64bits(f) }
func mathFloat(b uint64) float64 { return math.Float64frombits(b) }
