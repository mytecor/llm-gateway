package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func TestServeReleasesAPIListenerOnMetricsBindFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	apiAddress := unusedAddress(t)
	if err := serve(context.Background(), &http.Server{Addr: apiAddress}, &http.Server{Addr: occupied.Addr().String()}); err == nil {
		t.Fatal("expected metrics bind failure")
	}
	probe, err := net.Listen("tcp", apiAddress)
	if err != nil {
		t.Fatalf("failed startup leaked API listener: %v", err)
	}
	probe.Close()
}

func TestServeCancellationClosesBothListeners(t *testing.T) {
	apiAddress, metricsAddress := unusedAddress(t), unusedAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, &http.Server{Addr: apiAddress, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })}, &http.Server{Addr: metricsAddress})
	}()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		response, err := client.Get("http://" + apiAddress)
		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			break
		}
		select {
		case err := <-done:
			t.Fatalf("serve exited before accepting requests: %v", err)
		case <-deadline.C:
			t.Fatal("serve did not start")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("shutdown did not return")
	}
	for _, address := range []string{apiAddress, metricsAddress} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("shutdown leaked %s: %v", address, err)
		}
		listener.Close()
	}
}
