package orchestrator

import (
	"context"
	"encoding/json"
	"sync"
)

// StateCheckpointer defines persistence hooks for saving execution checkpoints.
type StateCheckpointer interface {
	SaveCheckpoint(ctx context.Context, taskID string, stepID string, stateSnapshot []byte) error
}

// InMemoryCheckpointer is a thread-safe in-memory checkpointer suitable for testing and local runs.
type InMemoryCheckpointer struct {
	mu          sync.RWMutex
	checkpoints map[string][][]byte
}

// NewInMemoryCheckpointer initializes an in-memory checkpointer.
func NewInMemoryCheckpointer() *InMemoryCheckpointer {
	return &InMemoryCheckpointer{
		checkpoints: make(map[string][][]byte),
	}
}

// SaveCheckpoint saves a serialized snapshot of task state.
func (c *InMemoryCheckpointer) SaveCheckpoint(_ context.Context, taskID string, _ string, stateSnapshot []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cpCopy := make([]byte, len(stateSnapshot))
	copy(cpCopy, stateSnapshot)
	c.checkpoints[taskID] = append(c.checkpoints[taskID], cpCopy)
	return nil
}

// GetCheckpoints returns all saved checkpoints for a given task ID.
func (c *InMemoryCheckpointer) GetCheckpoints(taskID string) [][]byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkpoints[taskID]
}

// TaskSnapshot is the JSON-serializable snapshot of a task state.
type TaskSnapshot struct {
	TaskID       string                 `json:"task_id"`
	TenantID     string                 `json:"tenant_id"`
	TokenBudget  int64                  `json:"token_budget"`
	TokensUsed   int64                  `json:"tokens_used"`
	NodeStatuses map[string]NodeStatus  `json:"node_statuses"`
	NodeOutputs  map[string]*StepOutput `json:"node_outputs"`
	SharedMemory map[string]any         `json:"shared_memory"`
}

// TaskState maintains the mutable, thread-safe runtime state across graph execution.
type TaskState struct {
	mu           sync.RWMutex
	TaskID       string
	TenantID     string
	TokenBudget  int64
	TokensUsed   int64
	NodeStatuses map[string]NodeStatus
	NodeOutputs  map[string]*StepOutput
	SharedMemory map[string]any
	checkpointer StateCheckpointer
}

// NewTaskState creates a fresh runtime state for an agent graph execution.
func NewTaskState(taskID, tenantID string, tokenBudget int64, cp StateCheckpointer) *TaskState {
	return &TaskState{
		TaskID:       taskID,
		TenantID:     tenantID,
		TokenBudget:  tokenBudget,
		NodeStatuses: make(map[string]NodeStatus),
		NodeOutputs:  make(map[string]*StepOutput),
		SharedMemory: make(map[string]any),
		checkpointer: cp,
	}
}

// GetOutput returns the recorded step output for the specified node ID if completed.
func (s *TaskState) GetOutput(nodeID string) (*StepOutput, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out, ok := s.NodeOutputs[nodeID]
	return out, ok
}

// GetStatus returns the current execution status of a node.
func (s *TaskState) GetStatus(nodeID string) NodeStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.NodeStatuses[nodeID]
}

// SetStatus updates the status of a specific node thread-safely.
func (s *TaskState) SetStatus(nodeID string, status NodeStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.NodeStatuses[nodeID] = status
}

// SetShared sets a key-value pair in shared memory for inter-node communication.
func (s *TaskState) SetShared(key string, val any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SharedMemory[key] = val
}

// GetShared retrieves a value from shared memory.
func (s *TaskState) GetShared(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.SharedMemory[key]
	return val, ok
}

// Snapshot serializes the current state into JSON bytes.
func (s *TaskState) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := TaskSnapshot{
		TaskID:       s.TaskID,
		TenantID:     s.TenantID,
		TokenBudget:  s.TokenBudget,
		TokensUsed:   s.TokensUsed,
		NodeStatuses: make(map[string]NodeStatus, len(s.NodeStatuses)),
		NodeOutputs:  make(map[string]*StepOutput, len(s.NodeOutputs)),
		SharedMemory: make(map[string]any, len(s.SharedMemory)),
	}

	for k, v := range s.NodeStatuses {
		snap.NodeStatuses[k] = v
	}
	for k, v := range s.NodeOutputs {
		snap.NodeOutputs[k] = v
	}
	for k, v := range s.SharedMemory {
		snap.SharedMemory[k] = v
	}

	return json.Marshal(snap)
}

// CommitStep atomically records step completion, tallies tokens, and triggers checkpointing.
func (s *TaskState) CommitStep(ctx context.Context, nodeID string, output *StepOutput) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.NodeOutputs[nodeID] = output
	s.NodeStatuses[nodeID] = StatusCompleted
	if output != nil {
		s.TokensUsed += output.TokensUsed
	}

	if s.TokenBudget > 0 && s.TokensUsed > s.TokenBudget {
		return ErrBudgetExceeded
	}

	if s.checkpointer != nil {
		// Create snapshot payload
		snap := TaskSnapshot{
			TaskID:       s.TaskID,
			TenantID:     s.TenantID,
			TokenBudget:  s.TokenBudget,
			TokensUsed:   s.TokensUsed,
			NodeStatuses: s.NodeStatuses,
			NodeOutputs:  s.NodeOutputs,
			SharedMemory: s.SharedMemory,
		}
		data, _ := json.Marshal(snap)
		return s.checkpointer.SaveCheckpoint(ctx, s.TaskID, nodeID, data)
	}
	return nil
}
