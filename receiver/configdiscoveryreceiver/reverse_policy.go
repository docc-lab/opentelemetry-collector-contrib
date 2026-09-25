package configdiscoveryreceiver

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
)

// Tomislav-RetCtx: reject inconsistent probability settings before SDK discovery.
// Keep these accepted values aligned with Blueprint's reverse_policy.go.
func validateReversePolicy(config map[string]interface{}) error {
	policy := "ttl"
	if value, present := config["reverse_policy"]; present {
		var ok bool
		policy, ok = value.(string)
		if !ok {
			return errors.New("config_map: reverse_policy must be a string")
		}
	}
	value, present := config["reverse_probability"]
	switch policy {
	case "ttl", "inverse_depth", "depth_linear", "depth_cubic": // Tomislav-RetCtx: + depth_cubic (Blueprint SDK)
		if present {
			return errors.New("config_map: reverse_probability requires reverse_policy: probability")
		}
	case "probability":
		if !present || !validReverseProbability(value) {
			return errors.New("config_map: probability policy requires finite reverse_probability in [0,1]")
		}
	default:
		return errors.New("config_map: reverse_policy must be ttl, probability, inverse_depth, depth_linear, or depth_cubic")
	}
	return nil
}

func validReverseProbability(value interface{}) bool {
	var probability float64
	var err error
	switch v := value.(type) {
	case float64:
		probability = v
	case int:
		probability = float64(v)
	case int64:
		probability = float64(v)
	case json.Number:
		probability, err = v.Float64()
	case string:
		probability, err = strconv.ParseFloat(v, 64)
	default:
		return false
	}
	return err == nil && !math.IsNaN(probability) && probability >= 0 && probability <= 1
}
