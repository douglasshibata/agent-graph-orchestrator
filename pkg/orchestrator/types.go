package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// NodeStatus defines the execution status of an individual node in the DAG.
type NodeStatus string

const (
	StatusPending   NodeStatus = "PENDING"
	StatusRunning   NodeStatus = "RUNNING"
	StatusCompleted NodeStatus = "COMPLETED"
	StatusFailed    NodeStatus = "FAILED"
	StatusSkipped   NodeStatus = "SKIPPED"
	StatusSuspended NodeStatus = "SUSPENDED" // Human-in-the-loop pause
)

var (
	ErrCycleDetected   = errors.New("graph contains cycles")
	ErrExecutionFailed = errors.New("execution terminated with node failures")
	ErrBudgetExceeded  = errors.New("token budget exceeded")
	ErrTimeoutExceeded = errors.New("step execution timeout")
)

// StepOutput encapsulates the result, token metrics, and execution metadata of a node run.
type StepOutput struct {
	Result        any            `json:"result"`
	TokensUsed    int64          `json:"tokens_used"`
	ExecutionTime time.Duration  `json:"execution_time"`
	Metadata      map[string]any `json:"metadata"`
}

// StepFunc represents the executable work unit of a graph node.
type StepFunc func(ctx context.Context, state *TaskState) (*StepOutput, error)

// ConditionFunc evaluates task state to dynamically determine if a node should execute or skip.
type ConditionFunc func(state *TaskState) bool

// GraphNode defines a discrete agent action, dependencies, execution logic, and retry limits.
type GraphNode struct {
	ID           string
	Dependencies []string
	Execute      StepFunc
	Condition    ConditionFunc // If returns false, node and dependants are SKIPPED
	Timeout      time.Duration
	MaxRetries   int
}

// Graph is the directed acyclic graph structure containing all nodes.
type Graph struct {
	Nodes map[string]*GraphNode
}

// NewGraph initializes an empty Directed Acyclic Graph.
func NewGraph() *Graph {
	return &Graph{
		Nodes: make(map[string]*GraphNode),
	}
}

// AddNode registers a node into the graph, returning an error if ID already exists.
func (g *Graph) AddNode(node *GraphNode) error {
	if node == nil || node.ID == "" {
		return errors.New("invalid node: node or node.ID cannot be empty")
	}
	if _, exists := g.Nodes[node.ID]; exists {
		return fmt.Errorf("node with ID %s already registered", node.ID)
	}
	g.Nodes[node.ID] = node
	return nil
}
