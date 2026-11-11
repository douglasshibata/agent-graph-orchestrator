package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Engine manages concurrent topological scheduling and execution of agent DAGs.
type Engine struct {
	MaxConcurrency int
}

// NewEngine creates a new DAG Execution Engine with bounded concurrency.
func NewEngine(maxConcurrency int) *Engine {
	if maxConcurrency <= 0 {
		maxConcurrency = 16
	}
	return &Engine{MaxConcurrency: maxConcurrency}
}

// ValidateDAG runs Kahn's algorithm to ensure dependencies exist and the graph is acyclic.
func (e *Engine) ValidateDAG(g *Graph) error {
	inDegree := make(map[string]int)
	for id := range g.Nodes {
		inDegree[id] = 0
	}
	for _, node := range g.Nodes {
		for _, dep := range node.Dependencies {
			if _, exists := g.Nodes[dep]; !exists {
				return fmt.Errorf("node %s references non-existent dependency %s", node.ID, dep)
			}
		}
		inDegree[node.ID] = len(node.Dependencies)
	}

	queue := make([]string, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	adj := make(map[string][]string)
	for _, node := range g.Nodes {
		for _, dep := range node.Dependencies {
			adj[dep] = append(adj[dep], node.ID)
		}
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		visited++

		for _, dependent := range adj[curr] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if visited != len(g.Nodes) {
		return ErrCycleDetected
	}
	return nil
}

type nodeNotification struct {
	nodeID string
	status NodeStatus
}

// Execute validates the DAG and runs all nodes respecting dependencies, conditions, and token budgets.
func (e *Engine) Execute(ctx context.Context, g *Graph, state *TaskState) error {
	if err := e.ValidateDAG(g); err != nil {
		return err
	}

	notifyChan := make(chan nodeNotification, len(g.Nodes)*2)
	inDegree := make(map[string]int)
	dependents := make(map[string][]string)

	for id, node := range g.Nodes {
		state.SetStatus(id, StatusPending)
		inDegree[id] = len(node.Dependencies)
		for _, dep := range node.Dependencies {
			dependents[dep] = append(dependents[dep], id)
		}
	}

	semaphore := make(chan struct{}, e.MaxConcurrency)
	var wg sync.WaitGroup
	errChan := make(chan error, len(g.Nodes))

	// Launch initially unblocked nodes (in-degree == 0)
	for id, deg := range inDegree {
		if deg == 0 {
			e.scheduleNode(ctx, g.Nodes[id], state, semaphore, &wg, notifyChan, errChan)
		}
	}

	// Dynamic Resolution Loop
	nodesRemaining := len(g.Nodes)
	for nodesRemaining > 0 {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case err := <-errChan:
			wg.Wait()
			return fmt.Errorf("%w: %v", ErrExecutionFailed, err)
		case notif := <-notifyChan:
			nodesRemaining--

			for _, nextID := range dependents[notif.nodeID] {
				if notif.status == StatusSkipped || notif.status == StatusFailed {
					// Cascade cancellation to dependencies if not already finalized
					currentStatus := state.GetStatus(nextID)
					if currentStatus == StatusPending {
						state.SetStatus(nextID, StatusSkipped)
						notifyChan <- nodeNotification{nodeID: nextID, status: StatusSkipped}
					}
					continue
				}

				inDegree[nextID]--
				if inDegree[nextID] == 0 {
					e.scheduleNode(ctx, g.Nodes[nextID], state, semaphore, &wg, notifyChan, errChan)
				}
			}
		}
	}

	wg.Wait()
	return nil
}

func (e *Engine) scheduleNode(
	ctx context.Context,
	node *GraphNode,
	state *TaskState,
	sem chan struct{},
	wg *sync.WaitGroup,
	notify chan<- nodeNotification,
	errChan chan<- error,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Evaluate branch condition
		if node.Condition != nil && !node.Condition(state) {
			state.SetStatus(node.ID, StatusSkipped)
			notify <- nodeNotification{nodeID: node.ID, status: StatusSkipped}
			return
		}

		// Acquire Concurrency Token
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			state.SetStatus(node.ID, StatusFailed)
			errChan <- ctx.Err()
			notify <- nodeNotification{nodeID: node.ID, status: StatusFailed}
			return
		}
		defer func() { <-sem }()

		state.SetStatus(node.ID, StatusRunning)

		timeout := node.Timeout
		if timeout == 0 {
			timeout = 60 * time.Second
		}
		stepCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		var out *StepOutput
		var err error

		for attempt := 0; attempt <= node.MaxRetries; attempt++ {
			if node.Execute != nil {
				out, err = node.Execute(stepCtx, state)
			} else {
				out = &StepOutput{Metadata: map[string]any{"noop": true}}
				err = nil
			}
			if err == nil {
				break
			}
			if stepCtx.Err() != nil {
				err = ErrTimeoutExceeded
				break
			}
		}

		if err != nil {
			state.SetStatus(node.ID, StatusFailed)
			errChan <- fmt.Errorf("node %s failed: %w", node.ID, err)
			notify <- nodeNotification{nodeID: node.ID, status: StatusFailed}
			return
		}

		if err := state.CommitStep(ctx, node.ID, out); err != nil {
			state.SetStatus(node.ID, StatusFailed)
			errChan <- err
			notify <- nodeNotification{nodeID: node.ID, status: StatusFailed}
			return
		}

		notify <- nodeNotification{nodeID: node.ID, status: StatusCompleted}
	}()
}
