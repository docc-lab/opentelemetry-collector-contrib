package configdiscoveryreceiver

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCheckpointRangeValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values map[string]interface{}
		valid  bool
	}{
		{"legacy", map[string]interface{}{"cpd": 6}, true},
		{"range", map[string]interface{}{"cpd_min": 2, "cpd_max": 6}, true},
		{"every_span", map[string]interface{}{"cpd_min": 1, "cpd_max": 1}, true},
		{"full_byte", map[string]interface{}{"cpd_min": 256, "cpd_max": 256}, true},
		{"strings", map[string]interface{}{"cpd_min": "2", "cpd_max": "6"}, true},
		{"only_min", map[string]interface{}{"cpd_min": 2}, false},
		{"only_max", map[string]interface{}{"cpd_max": 6}, false},
		{"zero", map[string]interface{}{"cpd_min": 0, "cpd_max": 6}, false},
		{"negative", map[string]interface{}{"cpd_min": -1, "cpd_max": 6}, false},
		{"reversed", map[string]interface{}{"cpd_min": 6, "cpd_max": 2}, false},
		{"too_large", map[string]interface{}{"cpd_min": 1, "cpd_max": 257}, false},
		{"fraction", map[string]interface{}{"cpd_min": 1.5, "cpd_max": 6}, false},
		{"nan", map[string]interface{}{"cpd_min": math.NaN(), "cpd_max": 6}, false},
		{"null", map[string]interface{}{"cpd_min": nil, "cpd_max": 6}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{ConfigMap: tc.values}
			cfg.Endpoint = "localhost:8080"
			if err := cfg.Validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}

func TestCheckpointAndReversePolicyConfigDiscovery(t *testing.T) {
	var cfg Config
	cm := confmap.NewFromStringMap(map[string]interface{}{
		"endpoint": "localhost:8080", "config_map": map[string]interface{}{
			"cpd_min": 2, "cpd_max": 6,
			"reverse_policy": "probability", "reverse_probability": 0.25,
		},
	})
	if err := cm.Unmarshal(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	r := &configDiscoveryReceiver{cfg: &cfg, settings: receivertest.NewNopSettings(typ)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.handleGetFullConfig(w, req, nil)
	}))
	defer server.Close()
	resp, err := server.Client().Get(server.URL + "/getFullConfig")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body configResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || body.Config["cpd_min"] != float64(2) || body.Config["cpd_max"] != float64(6) {
		t.Fatalf("SDK discovery response: status=%d, body=%v", resp.StatusCode, body)
	}
	if body.Config["reverse_policy"] != "probability" || body.Config["reverse_probability"] != 0.25 {
		t.Fatalf("SDK discovery lost reverse policy: %v", body)
	}
}
