// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/configdiscoveryreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/julienschmidt/httprouter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type configDiscoveryReceiver struct {
	settings   receiver.Settings
	cfg        *Config
	server     *http.Server
	shutdownWG sync.WaitGroup
}

type configResponse struct {
	Config map[string]interface{} `json:"config"`
}

func newConfigDiscoveryReceiver(params receiver.Settings, cfg Config) (receiver.Logs, error) {
	return &configDiscoveryReceiver{
		settings: params,
		cfg:      &cfg,
	}, nil
}

func (r *configDiscoveryReceiver) Start(ctx context.Context, host component.Host) error {
	// noop if server already exists
	if r.server != nil && r.server.Handler != nil {
		return nil
	}

	// create listener from config
	ln, err := r.cfg.ToListener(ctx)
	if err != nil {
		return err
	}

	// set up router
	router := httprouter.New()

	// Support GET /getConfig and GET /getFullConfig endpoints
	router.GET("/getConfig", r.handleGetConfig)
	router.GET("/getFullConfig", r.handleGetFullConfig)

	// HTTP server setup and configuration
	r.server, err = r.cfg.ToServer(ctx, host, r.settings.TelemetrySettings, router)
	if err != nil {
		return err
	}

	// shutdown
	r.shutdownWG.Add(1)
	go func() {
		defer r.shutdownWG.Done()
		if errHTTP := r.server.Serve(ln); !errors.Is(errHTTP, http.ErrServerClosed) && errHTTP != nil {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	}()

	r.settings.Logger.Info("Config Discovery receiver started",
		zap.String("endpoint", r.cfg.Endpoint))

	return nil
}

// Shutdown function manages receiver shutdown tasks.
func (r *configDiscoveryReceiver) Shutdown(_ context.Context) error {
	if r.server == nil {
		return nil
	}

	err := r.server.Close()
	r.shutdownWG.Wait()
	return err
}

// handleGetConfig handles the config discovery endpoint.
func (r *configDiscoveryReceiver) handleGetConfig(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse query parameters to get requested config properties
	queryParams := req.URL.Query()
	properties := queryParams["property"]

	config := r.discoverConfig(properties)

	response := configResponse{
		Config: config,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		r.settings.Logger.Error("Failed to encode config response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	r.settings.Logger.Debug("Config discovery request handled",
		zap.Strings("properties", properties),
		zap.Int("count", len(config)))
}

// discoverConfig retrieves the requested configuration properties.
// Returns a map where keys are property names and values are the config values.
// If a property is not found, it will be included in the result with a nil value.
func (r *configDiscoveryReceiver) discoverConfig(properties []string) map[string]interface{} {
	result := make(map[string]interface{})

	if r.cfg.ConfigMap == nil {
		// If no config map exists, return requested properties with nil values
		for _, prop := range properties {
			result[prop] = nil
		}
		return result
	}

	if len(properties) == 0 {
		// If no properties specified, return empty config
		return result
	}

	// Process each requested property
	for _, prop := range properties {
		if value, exists := r.cfg.ConfigMap[prop]; exists {
			result[prop] = value
		} else {
			// Property not found - return null to indicate it doesn't exist
			result[prop] = nil
		}
	}

	return result
}

// handleGetFullConfig handles the full config discovery endpoint.
func (r *configDiscoveryReceiver) handleGetFullConfig(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	var config map[string]interface{}

	if r.cfg.ConfigMap != nil {
		config = r.cfg.ConfigMap
	} else {
		config = make(map[string]interface{})
	}

	response := configResponse{
		Config: config,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		r.settings.Logger.Error("Failed to encode full config response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	r.settings.Logger.Debug("Full config discovery request handled",
		zap.Int("property_count", len(config)))
}

// RegisterLogsConsumer is required by the receiver interface but not used for this receiver.
func (r *configDiscoveryReceiver) RegisterLogsConsumer(consumer.Logs) {
	// This receiver doesn't consume logs, it only provides HTTP endpoints
}
