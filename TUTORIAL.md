# 📘 Tutorial Prático: Low-Level Go Agent Graph Orchestrator & Ephemeral Firecracker Sandbox

Este tutorial guia o desenvolvedor passo a passo na arquitetura, execução e integração do orquestrador determinístico de grafos de agentes em Go e seu executor de sandbox isolado via Firecracker MicroVM.

---

## 🎯 Objetivos de Aprendizagem
1. Compreender a execução determinística de grafos acíclicos dirigidos (DAG) para agentes de IA.
2. Monitorar consumo de tokens e persistir snapshots de estado com o `StateCheckpointer`.
3. Provisionar sandboxes efêmeros de baixo overhead isolados via Firecracker MicroVM e VirtIO Sockets (VSOCK).
4. Configurar regras de rede e egress firewall contra SSRF e exfiltração de metadados em nuvem.

---

## 🏗️ Pré-requisitos
- **Go 1.24+** instalado (`go version`).
- **Linux com suporte a KVM** para instâncias reais de Firecracker (`/dev/kvm`), ou o emulador de mock integrado para ambientes de teste e CI/CD.
- **Docker & Docker Compose** (opcional para execução conteinerizada).

---

## 🚀 Passo a Passo de Execução

### Passo 1: Executar a Suíte de Testes com Detecção de Race Conditions
Navegue até a pasta do projeto e rode os testes com `-race`:
```bash
cd agent-graph-orchestrator
go test -v -race ./...
```
Todos os 9 cenários de teste (validação de ciclos, execução paralela, orçamentação de tokens, isolamento de rede e integração E2E) devem retornar `PASS`.

---

### Passo 2: Executar a Demonstração via CLI
Para visualizar o orquestrador montando uma DAG de 3 nós (LLM -> RAG -> MicroVM Sandbox Python), execute:
```bash
go run ./cmd/orchestrator/main.go -mode=cli
```
Saída esperada:
```text
==================================================================
🚀 Low-Level Go Agent Graph Orchestrator & Ephemeral Sandbox
==================================================================
[+] Executing DAG with 3 nodes...
[✓] Execution completed successfully in 77ms
[*] Total Tokens Used: 350 / 1000
    - node_c_sandbox [COMPLETED] -> SANDBOX_OUT: print([0, 1, 1, 2, 3, 5, 8])
    - node_a_llm [COMPLETED] -> Intent extracted: compute_fibonacci
    - node_b_rag [COMPLETED] -> RAG context retrieved
[*] Checkpoints Recorded: 3
```

---

### Passo 3: Iniciar o Servidor REST e Submeter um Workflow
Inicie o servidor HTTP na porta 8080:
```bash
go run ./cmd/orchestrator/main.go -mode=server -port=8080
```

Em outro terminal, realize a chamada via `curl`:
```bash
curl -X POST http://localhost:8080/api/workflow/run \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "tarefa-001",
    "tenant_id": "empresa-xyz",
    "token_budget": 2000,
    "language": "python",
    "code_snippet": "print(sum([10, 20, 30, 40]))"
  }'
```

Resposta JSON esperada:
```json
{
  "task_id": "tarefa-001",
  "tenant_id": "empresa-xyz",
  "tokens_used": 220,
  "status": "COMPLETED",
  "node_statuses": {
    "llm_planner": "COMPLETED",
    "sandbox_executor": "COMPLETED"
  },
  "node_outputs": {
    "llm_planner": {
      "result": "Plan generated",
      "tokens_used": 100,
      "execution_time": 0,
      "metadata": null
    },
    "sandbox_executor": {
      "result": "SANDBOX_OUT: print(sum([10, 20, 30, 40]))",
      "tokens_used": 120,
      "execution_time": 450000,
      "metadata": {
        "duration": "120µs",
        "language": "python"
      }
    }
  }
}
```

---

### Passo 4: Executar via Docker Compose
Para subir o container isolado:
```bash
docker compose up --build -d
```
Verifique a saúde da aplicação:
```bash
curl http://localhost:8080/health
```
Saída:
```json
{"status":"UP","version":"1.0.0"}
```
Para finalizar:
```bash
docker compose down
```

---

## 🔒 Segurança de Rede e Firewall de Saída
O módulo `pkg/sandbox/network.go` gera comandos estritos de iptables para impedir que código arbitrário alcance redes corporativas ou serviços de metadados da nuvem:
- Bloqueio de IP de metadados AWS/GCP/Azure: `169.254.169.254/32`
- Bloqueio de RFC 1918 (redes locais privadas): `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`
- Liberação controlada para saída à Internet pública apenas quando estritamente configurado.
