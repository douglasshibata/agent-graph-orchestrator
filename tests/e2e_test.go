package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"agent-graph-orchestrator/internal/integration"
	"agent-graph-orchestrator/pkg/orchestrator"
	"agent-graph-orchestrator/pkg/sandbox"
)

func TestE2E_CompleteAgentWorkflow(t *testing.T) {
	// 1. Initialize Mock Sandbox
	tmpDir, err := os.MkdirTemp("", "e2e_sb_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mockServer, err := sandbox.NewMockFirecrackerServer(tmpDir)
	if err != nil {
		t.Fatalf("failed starting mock server: %v", err)
	}
	defer mockServer.Close()

	sb := sandbox.NewMicroVMSandbox("vm_e2e_01", "tap_e2e")
	sb.SocketPath = mockServer.SocketPath
	sb.JailRoot = tmpDir

	// 2. Define Multi-Step Graph:
	//    Input -> LLM_Plan -> (Branch 1: RAG Doc / Branch 2: Web Search) -> Python Sandbox Sandbox_Run -> Final Summary
	g := orchestrator.NewGraph()

	_ = g.AddNode(&orchestrator.GraphNode{
		ID: "llm_plan",
		Execute: func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			state.SetShared("plan_approved", true)
			state.SetShared("chosen_branch", "rag")
			return &orchestrator.StepOutput{
				Result:     "Plan: Use RAG then execute sandbox code",
				TokensUsed: 75,
			}, nil
		},
	})

	_ = g.AddNode(&orchestrator.GraphNode{
		ID:           "rag_retrieval",
		Dependencies: []string{"llm_plan"},
		Condition: func(state *orchestrator.TaskState) bool {
			branch, _ := state.GetShared("chosen_branch")
			return branch == "rag"
		},
		Execute: func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			state.SetShared("retrieved_data", "x = 42; y = 58; print(x + y)")
			return &orchestrator.StepOutput{
				Result:     "Retrieved Python code formula",
				TokensUsed: 50,
			}, nil
		},
	})

	_ = g.AddNode(&orchestrator.GraphNode{
		ID:           "web_search",
		Dependencies: []string{"llm_plan"},
		Condition: func(state *orchestrator.TaskState) bool {
			branch, _ := state.GetShared("chosen_branch")
			return branch == "web"
		},
		Execute: func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			return &orchestrator.StepOutput{Result: "Web search results", TokensUsed: 40}, nil
		},
	})

	// Sandbox execution depends on RAG
	adapter := integration.NewSandboxStepAdapter(sb, "python", "x = 42; y = 58; print(x + y)", 256, 110)
	_ = g.AddNode(&orchestrator.GraphNode{
		ID:           "sandbox_run",
		Dependencies: []string{"rag_retrieval"},
		Execute:      adapter.BuildStepFunc(),
	})

	// Final synthesis depends on sandbox_run
	_ = g.AddNode(&orchestrator.GraphNode{
		ID:           "final_summary",
		Dependencies: []string{"sandbox_run"},
		Execute: func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			sbOut, ok := state.GetOutput("sandbox_run")
			if !ok {
				t.Fatalf("missing sandbox_run output in final_summary")
			}
			return &orchestrator.StepOutput{
				Result:     "Agent finished task with output: " + sbOut.Result.(string),
				TokensUsed: 40,
			}, nil
		},
	})

	// 3. Execution
	cp := orchestrator.NewInMemoryCheckpointer()
	state := orchestrator.NewTaskState("task-e2e-888", "tenant-corp", 1500, cp)
	engine := orchestrator.NewEngine(4)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := engine.Execute(ctx, g, state); err != nil {
		t.Fatalf("e2e execution failed: %v", err)
	}

	// 4. Assertions
	if state.GetStatus("llm_plan") != orchestrator.StatusCompleted {
		t.Errorf("llm_plan status mismatch: %v", state.GetStatus("llm_plan"))
	}
	if state.GetStatus("rag_retrieval") != orchestrator.StatusCompleted {
		t.Errorf("rag_retrieval status mismatch: %v", state.GetStatus("rag_retrieval"))
	}
	if state.GetStatus("web_search") != orchestrator.StatusSkipped {
		t.Errorf("web_search expected SKIPPED, got: %v", state.GetStatus("web_search"))
	}
	if state.GetStatus("sandbox_run") != orchestrator.StatusCompleted {
		t.Errorf("sandbox_run status mismatch: %v", state.GetStatus("sandbox_run"))
	}
	if state.GetStatus("final_summary") != orchestrator.StatusCompleted {
		t.Errorf("final_summary status mismatch: %v", state.GetStatus("final_summary"))
	}

	// 75 + 50 + 110 + 40 = 275
	if state.TokensUsed != 275 {
		t.Errorf("expected 275 tokens used, got: %d", state.TokensUsed)
	}

	checkpoints := cp.GetCheckpoints("task-e2e-888")
	if len(checkpoints) != 4 {
		t.Errorf("expected 4 checkpoints (completed nodes), got: %d", len(checkpoints))
	}
}
