package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"agent-graph-orchestrator/pkg/orchestrator"
)

func TestEngine_ValidateDAG_CycleDetection(t *testing.T) {
	engine := orchestrator.NewEngine(4)

	// Valid DAG: A -> B -> C
	gValid := orchestrator.NewGraph()
	_ = gValid.AddNode(&orchestrator.GraphNode{ID: "A"})
	_ = gValid.AddNode(&orchestrator.GraphNode{ID: "B", Dependencies: []string{"A"}})
	_ = gValid.AddNode(&orchestrator.GraphNode{ID: "C", Dependencies: []string{"B"}})

	if err := engine.ValidateDAG(gValid); err != nil {
		t.Fatalf("expected valid DAG to pass, got: %v", err)
	}

	// Cyclic DAG: A -> B -> C -> A
	gCycle := orchestrator.NewGraph()
	_ = gCycle.AddNode(&orchestrator.GraphNode{ID: "A", Dependencies: []string{"C"}})
	_ = gCycle.AddNode(&orchestrator.GraphNode{ID: "B", Dependencies: []string{"A"}})
	_ = gCycle.AddNode(&orchestrator.GraphNode{ID: "C", Dependencies: []string{"B"}})

	err := engine.ValidateDAG(gCycle)
	if !errors.Is(err, orchestrator.ErrCycleDetected) {
		t.Fatalf("expected ErrCycleDetected, got: %v", err)
	}

	// Missing dependency
	gMissing := orchestrator.NewGraph()
	_ = gMissing.AddNode(&orchestrator.GraphNode{ID: "A", Dependencies: []string{"UNKNOWN"}})
	if err := engine.ValidateDAG(gMissing); err == nil {
		t.Fatalf("expected error for non-existent dependency, got nil")
	}
}

func TestEngine_Execute_ParallelAndDependencies(t *testing.T) {
	engine := orchestrator.NewEngine(4)
	cp := orchestrator.NewInMemoryCheckpointer()
	state := orchestrator.NewTaskState("task-par-1", "tenant-1", 1000, cp)

	g := orchestrator.NewGraph()
	var aFinished, bFinished atomic.Bool

	_ = g.AddNode(&orchestrator.GraphNode{
		ID: "A",
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			time.Sleep(30 * time.Millisecond)
			aFinished.Store(true)
			return &orchestrator.StepOutput{Result: "A-done", TokensUsed: 10}, nil
		},
	})

	_ = g.AddNode(&orchestrator.GraphNode{
		ID: "B",
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			time.Sleep(30 * time.Millisecond)
			bFinished.Store(true)
			return &orchestrator.StepOutput{Result: "B-done", TokensUsed: 20}, nil
		},
	})

	_ = g.AddNode(&orchestrator.GraphNode{
		ID:           "C",
		Dependencies: []string{"A", "B"},
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			if !aFinished.Load() || !bFinished.Load() {
				return nil, errors.New("dependencies A and B must finish before C")
			}
			return &orchestrator.StepOutput{Result: "C-done", TokensUsed: 30}, nil
		},
	})

	ctx := context.Background()
	if err := engine.Execute(ctx, g, state); err != nil {
		t.Fatalf("unexpected execution error: %v", err)
	}

	if state.GetStatus("C") != orchestrator.StatusCompleted {
		t.Fatalf("expected C to be COMPLETED, got: %v", state.GetStatus("C"))
	}
	if state.TokensUsed != 60 {
		t.Fatalf("expected 60 tokens, got: %d", state.TokensUsed)
	}
	if len(cp.GetCheckpoints("task-par-1")) != 3 {
		t.Fatalf("expected 3 checkpoints, got: %d", len(cp.GetCheckpoints("task-par-1")))
	}
}

func TestEngine_Execute_ConditionAndCascadeSkip(t *testing.T) {
	engine := orchestrator.NewEngine(4)
	state := orchestrator.NewTaskState("task-skip-1", "tenant-1", 1000, nil)

	g := orchestrator.NewGraph()
	_ = g.AddNode(&orchestrator.GraphNode{
		ID: "A",
		Condition: func(s *orchestrator.TaskState) bool {
			return false // Should skip
		},
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			return &orchestrator.StepOutput{Result: "A"}, nil
		},
	})

	_ = g.AddNode(&orchestrator.GraphNode{
		ID:           "B",
		Dependencies: []string{"A"},
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			return &orchestrator.StepOutput{Result: "B"}, nil
		},
	})

	ctx := context.Background()
	if err := engine.Execute(ctx, g, state); err != nil {
		t.Fatalf("unexpected execution error: %v", err)
	}

	if state.GetStatus("A") != orchestrator.StatusSkipped {
		t.Fatalf("expected A to be SKIPPED, got: %v", state.GetStatus("A"))
	}
	if state.GetStatus("B") != orchestrator.StatusSkipped {
		t.Fatalf("expected B to be SKIPPED due to cascade, got: %v", state.GetStatus("B"))
	}
}

func TestEngine_Execute_BudgetExceeded(t *testing.T) {
	engine := orchestrator.NewEngine(4)
	// Token budget = 50
	state := orchestrator.NewTaskState("task-budget-1", "tenant-1", 50, nil)

	g := orchestrator.NewGraph()
	_ = g.AddNode(&orchestrator.GraphNode{
		ID: "A",
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			return &orchestrator.StepOutput{Result: "Heavy LLM", TokensUsed: 100}, nil
		},
	})

	ctx := context.Background()
	err := engine.Execute(ctx, g, state)
	if err == nil {
		t.Fatalf("expected budget exceeded error, got nil")
	}
	if !errors.Is(err, orchestrator.ErrExecutionFailed) {
		t.Fatalf("expected ErrExecutionFailed wrapping ErrBudgetExceeded, got: %v", err)
	}
}

func TestEngine_Execute_NodeRetryAndTimeout(t *testing.T) {
	engine := orchestrator.NewEngine(2)
	state := orchestrator.NewTaskState("task-retry-1", "tenant-1", 500, nil)

	g := orchestrator.NewGraph()
	var attempts atomic.Int32

	_ = g.AddNode(&orchestrator.GraphNode{
		ID:         "FlakyNode",
		MaxRetries: 2,
		Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			cnt := attempts.Add(1)
			if cnt < 3 {
				return nil, errors.New("transient error")
			}
			return &orchestrator.StepOutput{Result: "SuccessOnAttempt3", TokensUsed: 10}, nil
		},
	})

	ctx := context.Background()
	if err := engine.Execute(ctx, g, state); err != nil {
		t.Fatalf("expected retry to succeed on 3rd attempt, got err: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got: %d", attempts.Load())
	}
	if state.GetStatus("FlakyNode") != orchestrator.StatusCompleted {
		t.Fatalf("expected COMPLETED status, got: %v", state.GetStatus("FlakyNode"))
	}
}
