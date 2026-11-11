package integration

import (
	"context"
	"fmt"
	"time"

	"agent-graph-orchestrator/pkg/orchestrator"
	"agent-graph-orchestrator/pkg/sandbox"
)

// SandboxStepAdapter binds a Sandbox instance to an orchestrator StepFunc.
type SandboxStepAdapter struct {
	Sandbox       sandbox.Sandbox
	Language      string
	SourceCode    string
	MemoryLimitMB int
	EstimatedCost int64
}

// NewSandboxStepAdapter creates an adapter for executing code in a sandbox as a graph step.
func NewSandboxStepAdapter(sb sandbox.Sandbox, lang, code string, memMB int, tokens int64) *SandboxStepAdapter {
	if memMB <= 0 {
		memMB = 256
	}
	if tokens <= 0 {
		tokens = 50
	}
	return &SandboxStepAdapter{
		Sandbox:       sb,
		Language:      lang,
		SourceCode:    code,
		MemoryLimitMB: memMB,
		EstimatedCost: tokens,
	}
}

// BuildStepFunc returns the orchestrator.StepFunc for this sandbox action.
func (a *SandboxStepAdapter) BuildStepFunc() orchestrator.StepFunc {
	return func(ctx context.Context, state *orchestrator.TaskState) (*orchestrator.StepOutput, error) {
		start := time.Now()

		req := sandbox.ExecutionRequest{
			Language:      a.Language,
			SourceCode:    a.SourceCode,
			Timeout:       30 * time.Second,
			MemoryLimitMB: a.MemoryLimitMB,
			EnvironmentVars: map[string]string{
				"TASK_ID":   state.TaskID,
				"TENANT_ID": state.TenantID,
			},
		}

		res, err := a.Sandbox.ExecuteInGuest(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("sandbox execution error: %w", err)
		}

		if res.ExitCode != 0 {
			return nil, fmt.Errorf("process failed with exit code %d: %s", res.ExitCode, res.Stderr)
		}

		return &orchestrator.StepOutput{
			Result:        res.Stdout,
			TokensUsed:    a.EstimatedCost,
			ExecutionTime: time.Since(start),
			Metadata: map[string]any{
				"language": a.Language,
				"duration": res.Duration.String(),
			},
		}, nil
	}
}
