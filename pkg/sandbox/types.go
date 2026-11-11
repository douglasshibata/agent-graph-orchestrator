package sandbox

import (
	"context"
	"time"
)

// ExecutionRequest represents the sandboxed execution workload.
type ExecutionRequest struct {
	Language        string            `json:"language"` // "python", "bash", "node"
	SourceCode      string            `json:"source_code"`
	Timeout         time.Duration     `json:"timeout"`
	MemoryLimitMB   int               `json:"memory_limit_mb"`
	EnvironmentVars map[string]string `json:"env_vars"`
}

// ExecutionResult represents the output from executing code inside the isolated environment.
type ExecutionResult struct {
	Stdout   string        `json:"stdout"`
	Stderr   string        `json:"stderr"`
	ExitCode int           `json:"exit_code"`
	Duration time.Duration `json:"duration"`
}

// GuestAgentCommand represents the command sent to the guest agent listening on VSOCK.
type GuestAgentCommand struct {
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Stdin   string            `json:"stdin"`
	Timeout int               `json:"timeout_seconds"`
	Env     map[string]string `json:"env"`
}

// GuestAgentResponse represents the execution response returned from the guest agent.
type GuestAgentResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

// Sandbox defines the lifecycle interface for isolated microVM or container environments.
type Sandbox interface {
	Start(ctx context.Context) error
	ExecuteInGuest(ctx context.Context, req ExecutionRequest) (*ExecutionResult, error)
	Teardown() error
}
