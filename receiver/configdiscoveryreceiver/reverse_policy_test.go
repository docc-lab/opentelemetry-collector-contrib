package configdiscoveryreceiver

import (
	"encoding/json"
	"math"
	"testing"
)

// Tomislav-RetCtx: config validation must match the SDK's accepted policies and
// reject missing, nonfinite, or irrelevant probability values before rollout.
func TestReversePolicyValidation(t *testing.T) {
	valid := []map[string]interface{}{
		nil, {"reverse_policy": "ttl"}, {"reverse_policy": "inverse_depth"}, {"reverse_policy": "depth_linear"}, {"reverse_policy": "depth_cubic"},
		{"reverse_policy": "probability", "reverse_probability": 0},
		{"reverse_policy": "probability", "reverse_probability": int64(1)},
		{"reverse_policy": "probability", "reverse_probability": 0.25},
		{"reverse_policy": "probability", "reverse_probability": "0.25"},
		{"reverse_policy": "probability", "reverse_probability": json.Number("0.25")},
	}
	invalid := []map[string]interface{}{
		{"reverse_policy": ""}, {"reverse_policy": "invalid"}, {"reverse_policy": 1}, {"reverse_policy": nil},
		{"reverse_probability": 0.5}, {"reverse_policy": "probability"},
		{"reverse_policy": "ttl", "reverse_probability": nil},
		{"reverse_policy": "inverse_depth", "reverse_probability": 0.5},
		{"reverse_policy": "depth_linear", "reverse_probability": 0.5},
		{"reverse_policy": "depth_cubic", "reverse_probability": 0.5},
	}
	for _, value := range []interface{}{-1, 1.1, math.Inf(1), math.NaN(), "NaN", "bad", nil, true} {
		invalid = append(invalid, map[string]interface{}{"reverse_policy": "probability", "reverse_probability": value})
	}
	for _, group := range []struct {
		configs []map[string]interface{}
		valid   bool
	}{{valid, true}, {invalid, false}} {
		for _, config := range group.configs {
			cfg := &Config{ConfigMap: config}
			cfg.Endpoint = "localhost:8080"
			if err := cfg.Validate(); (err == nil) != group.valid {
				t.Fatalf("config=%v valid=%v error=%v", config, group.valid, err)
			}
		}
	}
}
