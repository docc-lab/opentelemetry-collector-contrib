// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ipdiscoveryreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/ipdiscoveryreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/julienschmidt/httprouter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type ipDiscoveryReceiver struct {
	settings   receiver.Settings
	cfg        *Config
	server     *http.Server
	shutdownWG sync.WaitGroup
}

type ipResponse struct {
	IP string `json:"ip"`
}

func newIPDiscoveryReceiver(params receiver.Settings, cfg Config) (receiver.Logs, error) {
	return &ipDiscoveryReceiver{
		settings: params,
		cfg:      &cfg,
	}, nil
}

func (r *ipDiscoveryReceiver) Start(ctx context.Context, host component.Host) error {
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

	// Only support GET /getIP endpoint
	router.GET("/getIP", r.handleGetIP)

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

	r.settings.Logger.Info("IP Discovery receiver started",
		zap.String("endpoint", r.cfg.Endpoint))

	return nil
}

// Shutdown function manages receiver shutdown tasks.
func (r *ipDiscoveryReceiver) Shutdown(_ context.Context) error {
	if r.server == nil {
		return nil
	}

	err := r.server.Close()
	r.shutdownWG.Wait()
	return err
}

// handleGetIP handles the IP discovery endpoint.
func (r *ipDiscoveryReceiver) handleGetIP(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	ip := r.discoverIP()

	response := ipResponse{
		IP: ip,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		r.settings.Logger.Error("Failed to encode IP response", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	r.settings.Logger.Debug("IP discovery request handled", zap.String("ip", ip))
}

// discoverIP attempts to discover the agent's IP address using multiple methods.
func (r *ipDiscoveryReceiver) discoverIP() string {
	// Method 1: Check environment variables (Kubernetes)
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		r.settings.Logger.Debug("Using POD_IP from environment", zap.String("ip", podIP))
		return podIP
	}

	if hostIP := os.Getenv("HOST_IP"); hostIP != "" {
		r.settings.Logger.Debug("Using HOST_IP from environment", zap.String("ip", hostIP))
		return hostIP
	}

	// Method 2: Network interface detection
	if ip := r.getLocalIP(); ip != "" {
		r.settings.Logger.Debug("Using local IP from network interface", zap.String("ip", ip))
		return ip
	}

	// Method 3: Fallback to localhost
	r.settings.Logger.Debug("Using fallback IP", zap.String("ip", "localhost"))
	return "localhost"
}

// getLocalIP attempts to get the local IP address from network interfaces.
func (r *ipDiscoveryReceiver) getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		r.settings.Logger.Debug("Failed to get interface addresses", zap.Error(err))
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip := ipnet.IP.String()
				// Skip localhost and private network addresses that might not be useful
				if !strings.HasPrefix(ip, "127.") && !strings.HasPrefix(ip, "169.254.") {
					return ip
				}
			}
		}
	}

	return ""
}

// RegisterLogsConsumer is required by the receiver interface but not used for this receiver.
func (r *ipDiscoveryReceiver) RegisterLogsConsumer(consumer.Logs) {
	// This receiver doesn't consume logs, it only provides HTTP endpoints
}
