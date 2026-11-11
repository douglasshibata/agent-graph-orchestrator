package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// MicroVMSandbox manages an ephemeral Firecracker MicroVM jail process and communicates over Unix sockets & VSOCK.
type MicroVMSandbox struct {
	ID         string
	SocketPath string
	JailRoot   string
	TapDevice  string
	cmd        *exec.Cmd
	httpClient *http.Client

	// CustomDialer allows overriding Unix socket connection (used for mocking & unit testing)
	CustomDialer func(ctx context.Context, network, addr string) (net.Conn, error)
	// CustomVsockDialer allows overriding VSOCK connection (used for mocking & unit testing)
	CustomVsockDialer func(ctx context.Context, network, addr string) (net.Conn, error)
	// CustomCommandStarter allows mocking the jailer/firecracker execution
	CustomCommandStarter func(ctx context.Context, cmd *exec.Cmd) error
}

// NewMicroVMSandbox initializes a new MicroVMSandbox controller.
func NewMicroVMSandbox(id string, tapDevice string) *MicroVMSandbox {
	jailRoot := fmt.Sprintf("/srv/jailer/firecracker/%s/root", id)
	socketPath := filepath.Join(jailRoot, "run", "firecracker.socket")

	sandbox := &MicroVMSandbox{
		ID:         id,
		SocketPath: socketPath,
		JailRoot:   jailRoot,
		TapDevice:  tapDevice,
	}

	sandbox.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if sandbox.CustomDialer != nil {
					return sandbox.CustomDialer(ctx, network, addr)
				}
				return (&net.Dialer{}).DialContext(ctx, "unix", sandbox.SocketPath)
			},
		},
		Timeout: 5 * time.Second,
	}

	return sandbox
}

// Start spawns the jailer wrapper process, configures kernel, drive, resources, network, and boots the microVM.
func (s *MicroVMSandbox) Start(ctx context.Context) error {
	if s.CustomCommandStarter == nil {
		// Ensure run directory exists
		if err := os.MkdirAll(filepath.Dir(s.SocketPath), 0755); err != nil {
			return err
		}
		_ = os.Remove(s.SocketPath)

		// Spawn firecracker process via jailer wrapper
		s.cmd = exec.CommandContext(ctx, "/usr/bin/jailer",
			"--id", s.ID,
			"--uid", "10053",
			"--gid", "10053",
			"--exec-file", "/usr/bin/firecracker",
			"--node", "0",
			"--",
			"--api-sock", "/run/firecracker.socket",
		)

		s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := s.cmd.Start(); err != nil {
			return fmt.Errorf("failed starting jailer: %w", err)
		}
	} else {
		if err := s.CustomCommandStarter(ctx, s.cmd); err != nil {
			return fmt.Errorf("failed starting jailer via custom starter: %w", err)
		}
	}

	// Wait for socket to become available
	if err := s.waitForSocket(ctx, 2500*time.Millisecond); err != nil {
		_ = s.Teardown()
		return fmt.Errorf("timeout waiting for firecracker socket: %w", err)
	}

	// 1. Configure Boot Source (Kernel)
	bootPayload := map[string]any{
		"kernel_image_path": "vmlinux.bin",
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off init=/init quiet nomodules",
	}
	if err := s.put("/boot-source", bootPayload); err != nil {
		return err
	}

	// 2. Attach Rootfs Drive (Overlay copy)
	drivePayload := map[string]any{
		"drive_id":       "rootfs",
		"path_on_host":   "rootfs.ext4",
		"is_root_device": true,
		"is_read_only":   false,
	}
	if err := s.put("/drives/rootfs", drivePayload); err != nil {
		return err
	}

	// 3. Configure Machine Resources
	machinePayload := map[string]any{
		"vcpu_count":   1,
		"mem_size_mib": 256,
	}
	if err := s.put("/machine-config", machinePayload); err != nil {
		return err
	}

	// 4. Attach Virtual Network Interface
	networkPayload := map[string]any{
		"iface_id":      "net1",
		"guest_mac":     "AA:FC:00:00:00:01",
		"host_dev_name": s.TapDevice,
	}
	if err := s.put("/network-interfaces/net1", networkPayload); err != nil {
		return err
	}

	// 5. Fire Instance Start Action
	actionPayload := map[string]string{"action_type": "InstanceStart"}
	if err := s.put("/actions", actionPayload); err != nil {
		return err
	}

	return nil
}

func (s *MicroVMSandbox) waitForSocket(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var conn net.Conn
		var err error
		if s.CustomDialer != nil {
			conn, err = s.CustomDialer(ctx, "unix", s.SocketPath)
		} else {
			conn, err = net.Dial("unix", s.SocketPath)
		}

		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("socket dial timed out")
}

func (s *MicroVMSandbox) put(endpoint string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, "http://localhost"+endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("firecracker api error on %s: HTTP %d", endpoint, resp.StatusCode)
	}
	return nil
}

// ExecuteInGuest sends code to the guest agent listening on VSOCK port 5252.
func (s *MicroVMSandbox) ExecuteInGuest(ctx context.Context, req ExecutionRequest) (*ExecutionResult, error) {
	start := time.Now()

	var conn net.Conn
	var err error
	if s.CustomVsockDialer != nil {
		conn, err = s.CustomVsockDialer(ctx, "unix", filepath.Join(s.JailRoot, "run", "v.sock"))
	} else {
		vsockPath := filepath.Join(s.JailRoot, "run", "v.sock")
		conn, err = net.Dial("unix", vsockPath)
	}
	if err != nil {
		return nil, fmt.Errorf("failed connecting to vsock: %w", err)
	}
	defer conn.Close()

	// Connect to Guest Agent Port 5252: "CONNECT 5252\n"
	if _, err := conn.Write([]byte("CONNECT 5252\n")); err != nil {
		return nil, err
	}

	// Read handshake response
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil || !bytes.HasPrefix(buf[:n], []byte("OK")) {
		return nil, fmt.Errorf("guest agent handshake failed")
	}

	cmdPayload := GuestAgentCommand{
		Command: GetInterpreter(req.Language),
		Args:    []string{"-c", req.SourceCode},
		Timeout: int(req.Timeout.Seconds()),
		Env:     req.EnvironmentVars,
	}

	if err := json.NewEncoder(conn).Encode(cmdPayload); err != nil {
		return nil, err
	}

	var guestResp GuestAgentResponse
	if err := json.NewDecoder(conn).Decode(&guestResp); err != nil {
		return nil, fmt.Errorf("reading response failed: %w", err)
	}

	return &ExecutionResult{
		Stdout:   guestResp.Stdout,
		Stderr:   guestResp.Stderr,
		ExitCode: guestResp.ExitCode,
		Duration: time.Since(start),
	}, nil
}

// GetInterpreter resolves the binary path for supported languages.
func GetInterpreter(lang string) string {
	switch lang {
	case "python", "python3":
		return "/usr/bin/python3"
	case "node", "javascript", "js":
		return "/usr/bin/node"
	case "bash", "sh":
		return "/bin/bash"
	default:
		return "/bin/sh"
	}
}

// Teardown kills the jailer/firecracker process tree and cleans up socket artifacts.
func (s *MicroVMSandbox) Teardown() error {
	if s.cmd != nil && s.cmd.Process != nil {
		// Terminate process tree inside the jail
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		_ = s.cmd.Wait()
	}

	// Cleanup directory and network handles
	_ = os.RemoveAll(filepath.Dir(s.SocketPath))
	return nil
}
