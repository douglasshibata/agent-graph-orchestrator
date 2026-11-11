package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-graph-orchestrator/internal/integration"
	"agent-graph-orchestrator/pkg/orchestrator"
	"agent-graph-orchestrator/pkg/sandbox"
)

// WorkflowRunRequest represents the HTTP payload to trigger a workflow.
type WorkflowRunRequest struct {
	TaskID      string `json:"task_id"`
	TenantID    string `json:"tenant_id"`
	TokenBudget int64  `json:"token_budget"`
	CodeSnippet string `json:"code_snippet"`
	Language    string `json:"language"`
}

// WorkflowRunResponse represents the outcome of an execution.
type WorkflowRunResponse struct {
	TaskID       string                               `json:"task_id"`
	TenantID     string                               `json:"tenant_id"`
	TokensUsed   int64                                `json:"tokens_used"`
	Status       string                               `json:"status"`
	NodeStatuses map[string]orchestrator.NodeStatus   `json:"node_statuses"`
	NodeOutputs  map[string]*orchestrator.StepOutput  `json:"node_outputs"`
	Error        string                               `json:"error,omitempty"`
}

func main() {
	mode := flag.String("mode", "cli", "Execution mode: 'cli' to run demo DAG, 'server' to start REST API")
	port := flag.Int("port", 8080, "HTTP server port (server mode)")
	flag.Parse()

	switch *mode {
	case "server":
		runServer(*port)
	case "cli":
		runCLIDemo()
	default:
		log.Fatalf("Unknown mode: %s. Use 'cli' or 'server'", *mode)
	}
}

func runCLIDemo() {
	fmt.Println("==================================================================")
	fmt.Println("🚀 Low-Level Go Agent Graph Orchestrator & Ephemeral Sandbox")
	fmt.Println("==================================================================")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Setup Mock Firecracker Environment for local sandbox execution
	mockDir, err := os.MkdirTemp("", "firecracker_demo_*")
	if err != nil {
		log.Fatalf("Failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(mockDir)

	mockServer, err := sandbox.NewMockFirecrackerServer(mockDir)
	if err != nil {
		log.Fatalf("Failed starting mock firecracker: %v", err)
	}
	defer mockServer.Close()

	sb := sandbox.NewMicroVMSandbox("vm_demo_01", "tap_demo0")
	sb.SocketPath = mockServer.SocketPath
	sb.JailRoot = mockDir

	// 2. Build DAG
	graph := orchestrator.NewGraph()

	// Node A: LLM Inference
	_ = graph.AddNode(&orchestrator.GraphNode{
		ID:           "node_a_llm",
		Dependencies: nil,
		Execute: func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			time.Sleep(50 * time.Millisecond)
			state.SetShared("query_intent", "compute_fibonacci")
			return &orchestrator.StepOutput{
				Result:     "Intent extracted: compute_fibonacci",
				TokensUsed: 120,
				Metadata:   map[string]any{"model": "gpt-4o-mini"},
			}, nil
		},
	})

	// Node B: RAG Retrieval
	_ = graph.AddNode(&orchestrator.GraphNode{
		ID:           "node_b_rag",
		Dependencies: nil,
		Execute: func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
			time.Sleep(75 * time.Millisecond)
			state.SetShared("rag_context", "def fib(n): return n if n <= 1 else fib(n-1)+fib(n-2)")
			return &orchestrator.StepOutput{
				Result:     "RAG context retrieved",
				TokensUsed: 80,
				Metadata:   map[string]any{"source": "codebase_kb"},
			}, nil
		},
	})

	// Node C: Sandbox Execution (Runs in Ephemeral MicroVM, dependent on A and B)
	toolAdapter := integration.NewSandboxStepAdapter(sb, "python", "print([0, 1, 1, 2, 3, 5, 8])", 256, 150)
	_ = graph.AddNode(&orchestrator.GraphNode{
		ID:           "node_c_sandbox",
		Dependencies: []string{"node_a_llm", "node_b_rag"},
		Condition: func(state *orchestrator.TaskState) bool {
			val, _ := state.GetShared("query_intent")
			return val == "compute_fibonacci"
		},
		Execute: toolAdapter.BuildStepFunc(),
	})

	// 3. Execution State & Engine
	checkpointer := orchestrator.NewInMemoryCheckpointer()
	state := orchestrator.NewTaskState("task-demo-1001", "tenant-alpha", 1000, checkpointer)

	engine := orchestrator.NewEngine(4)
	fmt.Printf("[+] Executing DAG with %d nodes...\n", len(graph.Nodes))

	start := time.Now()
	if err := engine.Execute(ctx, graph, state); err != nil {
		log.Fatalf("Execution error: %v", err)
	}

	fmt.Printf("[✓] Execution completed successfully in %v\n", time.Since(start))
	fmt.Printf("[*] Total Tokens Used: %d / %d\n", state.TokensUsed, state.TokenBudget)
	for id, status := range state.NodeStatuses {
		out, _ := state.GetOutput(id)
		res := "none"
		if out != nil {
			res = fmt.Sprintf("%v", out.Result)
		}
		fmt.Printf("    - %s [%s] -> %s\n", id, status, res)
	}
	fmt.Printf("[*] Checkpoints Recorded: %d\n", len(checkpointer.GetCheckpoints("task-demo-1001")))
}

func runServer(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP", "version": "1.0.0"})
	})

	mux.HandleFunc("/api/workflow/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req WorkflowRunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.TaskID == "" {
			req.TaskID = fmt.Sprintf("task-%d", time.Now().UnixNano())
		}
		if req.TenantID == "" {
			req.TenantID = "tenant-default"
		}
		if req.TokenBudget <= 0 {
			req.TokenBudget = 2000
		}
		if req.Language == "" {
			req.Language = "python"
		}
		if req.CodeSnippet == "" {
			req.CodeSnippet = "print('Workflow execution complete')"
		}

		// Setup temporary mock sandbox for request
		mockDir, err := os.MkdirTemp("", "sb_server_*")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer os.RemoveAll(mockDir)

		mockServer, err := sandbox.NewMockFirecrackerServer(mockDir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer mockServer.Close()

		sb := sandbox.NewMicroVMSandbox("vm_"+req.TaskID, "tap0")
		sb.SocketPath = mockServer.SocketPath
		sb.JailRoot = mockDir

		graph := orchestrator.NewGraph()
		_ = graph.AddNode(&orchestrator.GraphNode{
			ID: "llm_planner",
			Execute: func(ctx context.Context, s *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
				return &orchestrator.StepOutput{
					Result:     "Plan generated",
					TokensUsed: 100,
				}, nil
			},
		})

		toolAdapter := integration.NewSandboxStepAdapter(sb, req.Language, req.CodeSnippet, 256, 120)
		_ = graph.AddNode(&orchestrator.GraphNode{
			ID:           "sandbox_executor",
			Dependencies: []string{"llm_planner"},
			Execute:      toolAdapter.BuildStepFunc(),
		})

		cp := orchestrator.NewInMemoryCheckpointer()
		state := orchestrator.NewTaskState(req.TaskID, req.TenantID, req.TokenBudget, cp)
		engine := orchestrator.NewEngine(4)

		execErr := engine.Execute(r.Context(), graph, state)
		resp := WorkflowRunResponse{
			TaskID:       state.TaskID,
			TenantID:     state.TenantID,
			TokensUsed:   state.TokensUsed,
			Status:       "COMPLETED",
			NodeStatuses: state.NodeStatuses,
			NodeOutputs:  state.NodeOutputs,
		}
		if execErr != nil {
			resp.Status = "FAILED"
			resp.Error = execErr.Error()
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		log.Printf("Server listening on port %d...", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
