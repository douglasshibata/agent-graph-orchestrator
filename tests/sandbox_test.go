package tests

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"agent-graph-orchestrator/pkg/sandbox"
)

func TestSandbox_MockFirecrackerLifecycleAndVSOCK(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sb_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mockServer, err := sandbox.NewMockFirecrackerServer(tmpDir)
	if err != nil {
		t.Fatalf("failed starting mock server: %v", err)
	}
	defer mockServer.Close()

	sb := sandbox.NewMicroVMSandbox("vm_test_99", "tap99")
	sb.SocketPath = mockServer.SocketPath
	sb.JailRoot = tmpDir
	sb.CustomCommandStarter = func(ctx context.Context, cmd *exec.Cmd) error {
		// Mock starting jailer command
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test Start
	if err := sb.Start(ctx); err != nil {
		t.Fatalf("sandbox.Start failed: %v", err)
	}

	// Verify REST requests sent to Firecracker socket
	reqs := mockServer.GetRequests()

	expectedEndpoints := []string{
		"PUT /boot-source",
		"PUT /drives/rootfs",
		"PUT /machine-config",
		"PUT /network-interfaces/net1",
		"PUT /actions",
	}

	for _, ep := range expectedEndpoints {
		found := false
		for _, req := range reqs {
			if strings.Contains(req, ep) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected Firecracker REST call: %s in %v", ep, reqs)
		}
	}

	// Test ExecuteInGuest via VSOCK
	execReq := sandbox.ExecutionRequest{
		Language:        "python",
		SourceCode:      "print('Hello Firecracker')",
		Timeout:         10 * time.Second,
		MemoryLimitMB:   256,
		EnvironmentVars: map[string]string{"ENV_A": "1"},
	}

	res, err := sb.ExecuteInGuest(ctx, execReq)
	if err != nil {
		t.Fatalf("ExecuteInGuest failed: %v", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", res.ExitCode)
	}
	if !strings.Contains(res.Stdout, "Hello Firecracker") {
		t.Errorf("expected stdout to contain 'Hello Firecracker', got: %s", res.Stdout)
	}

	// Test Teardown
	if err := sb.Teardown(); err != nil {
		t.Fatalf("Teardown failed: %v", err)
	}
}

func TestSandbox_NetworkIsolationRules(t *testing.T) {
	cfg := sandbox.NetworkConfig{
		TapDevice: "tap_vm100",
		HostIP:    "172.28.10.1/30",
		GuestMAC:  "AA:FC:00:00:00:01",
		UID:       "10053",
		DryRun:    true,
	}

	nm := sandbox.NewNetworkManager(cfg)
	rules := nm.GenerateIptablesRules()

	expectedRules := []string{
		"iptables -N SANDBOX_tap_vm100",
		"iptables -A FORWARD -i tap_vm100 -j SANDBOX_tap_vm100",
		"iptables -A SANDBOX_tap_vm100 -d 169.254.169.254/32 -j DROP",
		"iptables -A SANDBOX_tap_vm100 -d 10.0.0.0/8 -j DROP",
		"iptables -A SANDBOX_tap_vm100 -d 172.16.0.0/12 -j DROP",
		"iptables -A SANDBOX_tap_vm100 -d 192.168.0.0/16 -j DROP",
		"iptables -A SANDBOX_tap_vm100 -m state --state ESTABLISHED,RELATED -j ACCEPT",
		"iptables -A SANDBOX_tap_vm100 -j ACCEPT",
	}

	if len(rules) != len(expectedRules) {
		t.Fatalf("expected %d rules, got %d", len(expectedRules), len(rules))
	}

	for i, r := range expectedRules {
		if rules[i] != r {
			t.Errorf("rule mismatch at index %d:\nexpected: %s\ngot:      %s", i, r, rules[i])
		}
	}

	if err := nm.SetupTap(); err != nil {
		t.Fatalf("SetupTap in dry run failed: %v", err)
	}
	if err := nm.ApplyEgressRules(); err != nil {
		t.Fatalf("ApplyEgressRules in dry run failed: %v", err)
	}
	if err := nm.TeardownTap(); err != nil {
		t.Fatalf("TeardownTap in dry run failed: %v", err)
	}
}

func TestSandbox_GetInterpreter(t *testing.T) {
	tests := []struct {
		lang     string
		expected string
	}{
		{"python", "/usr/bin/python3"},
		{"python3", "/usr/bin/python3"},
		{"node", "/usr/bin/node"},
		{"javascript", "/usr/bin/node"},
		{"bash", "/bin/bash"},
		{"sh", "/bin/bash"},
		{"unknown", "/bin/sh"},
	}

	for _, tt := range tests {
		got := sandbox.GetInterpreter(tt.lang)
		if got != tt.expected {
			t.Errorf("GetInterpreter(%q) = %q; want %q", tt.lang, got, tt.expected)
		}
	}
}
