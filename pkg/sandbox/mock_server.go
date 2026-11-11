package sandbox

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// MockFirecrackerServer emulates the Firecracker REST socket API and VSOCK listener for testing.
type MockFirecrackerServer struct {
	SocketPath string
	VsockPath  string

	restListener  net.Listener
	vsockListener net.Listener

	mu       sync.Mutex
	Requests []string

	closed chan struct{}
}

// GetRequests returns a thread-safe copy of all HTTP requests received.
func (s *MockFirecrackerServer) GetRequests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]string, len(s.Requests))
	copy(copied, s.Requests)
	return copied
}

// NewMockFirecrackerServer starts a mock Unix domain socket server and mock VSOCK listener.
func NewMockFirecrackerServer(baseDir string) (*MockFirecrackerServer, error) {
	runDir := filepath.Join(baseDir, "run")
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return nil, err
	}

	sockPath := filepath.Join(runDir, "firecracker.socket")
	vsockPath := filepath.Join(runDir, "v.sock")

	_ = os.Remove(sockPath)
	_ = os.Remove(vsockPath)

	restLn, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("failed listening on rest socket: %w", err)
	}

	vsockLn, err := net.Listen("unix", vsockPath)
	if err != nil {
		_ = restLn.Close()
		return nil, fmt.Errorf("failed listening on vsock: %w", err)
	}

	s := &MockFirecrackerServer{
		SocketPath:    sockPath,
		VsockPath:     vsockPath,
		restListener:  restLn,
		vsockListener: vsockLn,
		Requests:      make([]string, 0),
		closed:        make(chan struct{}),
	}

	go s.serveREST()
	go s.serveVSOCK()

	return s, nil
}

func (s *MockFirecrackerServer) serveREST() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.Requests = append(s.Requests, fmt.Sprintf("%s %s", r.Method, r.URL.Path))
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"OK"}`))
		}
	})

	server := &http.Server{Handler: mux}
	_ = server.Serve(s.restListener)
}

func (s *MockFirecrackerServer) serveVSOCK() {
	for {
		conn, err := s.vsockListener.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
				return
			}
		}

		go s.handleVsockConn(conn)
	}
}

func (s *MockFirecrackerServer) handleVsockConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	handshake, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	if !strings.HasPrefix(strings.TrimSpace(handshake), "CONNECT") {
		_, _ = conn.Write([]byte("ERR\n"))
		return
	}

	// Send handshake OK
	_, _ = conn.Write([]byte("OK 5252\n"))

	// Read JSON payload
	var cmd GuestAgentCommand
	dec := json.NewDecoder(reader)
	if err := dec.Decode(&cmd); err != nil {
		if err != io.EOF {
			return
		}
	}

	stdout := fmt.Sprintf("executed %s %v", cmd.Command, cmd.Args)
	if len(cmd.Args) >= 2 && cmd.Args[0] == "-c" {
		stdout = fmt.Sprintf("SANDBOX_OUT: %s", cmd.Args[1])
	}

	resp := GuestAgentResponse{
		Stdout:   stdout,
		Stderr:   "",
		ExitCode: 0,
	}

	_ = json.NewEncoder(conn).Encode(resp)
}

// Close shuts down the mock listeners and deletes socket files.
func (s *MockFirecrackerServer) Close() {
	close(s.closed)
	if s.restListener != nil {
		_ = s.restListener.Close()
	}
	if s.vsockListener != nil {
		_ = s.vsockListener.Close()
	}
	_ = os.Remove(s.SocketPath)
	_ = os.Remove(s.VsockPath)
}
