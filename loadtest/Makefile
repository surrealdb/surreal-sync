# Makefile for distributed load testing
#
# Use bash for shell commands (required for jq with startswith filter in k8s-wait-for-completion)
SHELL := /bin/bash

# Usage:
#   make docker-build    # Build the surreal-sync Docker image
#   make generate        # Generate Docker Compose configuration
#   make run             # Run the load test with monitoring
#   make clean           # Clean up containers and volumes
#
# The test pipeline runs: populate -> sync -> verify
# Each stage must complete successfully before the next starts.
#
# =============================================================================
# Common Workflow (Automated)
# =============================================================================
#
# The simplest way to run a full test. This command:
#   1. Builds the Docker image
#   2. Generates docker-compose.loadtest.yml
#   3. Starts all containers (populate -> sync -> verify)
#   4. Monitors progress, exits early on failure
#   5. Prints summary and cleans up on success
#
#   cd loadtest && make down; make run SOURCE=mysql TIMEOUT=180
#
# The `run` target blocks until completion or timeout. Exit code 0 = success.
#
# =============================================================================
# Manual Workflow (Step-by-Step Debugging)
# =============================================================================
#
# Use this when you need to inspect state between stages or debug failures.
#
# Step 1: Build and start containers in background
#   make run-detached SOURCE=mysql
#
# Step 2: Monitor progress (run in a separate terminal or poll periodically)
#   make status                    # Shows container states (running/exited)
#   make logs                      # Follow all container logs (Ctrl+C to stop)
#
# Step 3: Wait for completion
#   - Populate containers exit first (populate-1, populate-2)
#   - Sync container exits after all populates complete
#   - Verify containers exit after sync completes
#   - Check `make status` until all show "Exited (0)"
#
# Step 4: Check verification results
#   make logs-verify               # Shows matched/mismatched/missing counts
#
# Step 5: Inspect specific container logs if needed
#   make logs-populate             # Populate container output
#   make logs-sync                 # Sync container output
#
# Step 6: Clean up
#   make down                      # Stop and remove containers
#
# =============================================================================
# Interactive Debugging (Database Access)
# =============================================================================
#
# Connect to MySQL while containers are running:
#   cd output && docker-compose -f docker-compose.loadtest.yml exec mysql \
#     mysql -uroot -proot loadtest
#
# Connect to SurrealDB while containers are running:
#   cd output && docker-compose -f docker-compose.loadtest.yml exec surrealdb \
#     /surreal sql --endpoint http://localhost:8000 --user root --pass root \
#     --namespace loadtest --database test
#

.PHONY: docker-build generate run run-ci run-detached clean clean-all logs logs-verify logs-sync logs-populate status help \
	test-dry-run-docker-compose test-dry-run-kubernetes \
	kind-create kind-delete kind-load kind-load-source-image k8s-run k8s-clean k8s-wait-for-completion \
	verify-aggregator test-all-sources down wait-for-completion wait-for-start test-scripts

# Configuration (override with environment variables)
SCHEMA ?= ./config/schema.yaml
SOURCE ?= mysql
PRESET ?= small
WORKERS ?= 2
ROW_COUNT ?= 1000
OUTPUT_DIR ?= ./output
IMAGE_NAME ?= surreal-sync:latest
KIND_CLUSTER ?= loadtest
PLATFORM ?= docker-compose

# Timeout and polling configuration
TIMEOUT ?= 300
POLL_INTERVAL ?= 5

# Help target
help:
	@echo "Distributed Load Testing Makefile"
	@echo ""
	@echo "Main Targets:"
	@echo "  docker-build   Build the surreal-sync Docker image"
	@echo "  generate       Generate Docker Compose configuration"
	@echo "  run            Run the load test with monitoring (detects failures early)"
	@echo "  run-ci         Run load test via CI script with JSON metrics output"
	@echo "  run-detached   Run in background without monitoring"
	@echo "  down           Stop and remove all containers"
	@echo "  clean          Clean up containers, volumes, and generated files"
	@echo "  clean-all      Force cleanup of all loadtest resources (use after failed run-ci)"
	@echo "  test-scripts   Run unit tests for CI scripts (no external deps)"
	@echo ""
	@echo "Debugging Targets:"
	@echo "  logs           Show logs from all containers (follow mode)"
	@echo "  logs-verify    Show verification results (matched/mismatched/missing)"
	@echo "  logs-sync      Show sync container logs"
	@echo "  logs-populate  Show populate container logs"
	@echo "  status         Show container status and recent logs"
	@echo ""
	@echo "Configuration (environment variables):"
	@echo "  SCHEMA=$(SCHEMA)"
	@echo "  SOURCE=$(SOURCE)"
	@echo "  PRESET=$(PRESET)"
	@echo "  WORKERS=$(WORKERS)"
	@echo "  ROW_COUNT=$(ROW_COUNT)"
	@echo "  OUTPUT_DIR=$(OUTPUT_DIR)"
	@echo "  IMAGE_NAME=$(IMAGE_NAME)"
	@echo "  TIMEOUT=$(TIMEOUT) (seconds)"
	@echo "  POLL_INTERVAL=$(POLL_INTERVAL) (seconds)"
	@echo ""
	@echo "Examples:"
	@echo "  make down; make run SOURCE=mysql TIMEOUT=180  # Full MySQL test"
	@echo "  make run SOURCE=postgresql WORKERS=4          # PostgreSQL with 4 containers"
	@echo "  make run-detached && make logs-verify         # Run detached, check results"
	@echo "  make run PRESET=large ROW_COUNT=10000 TIMEOUT=600"

# Build the Docker image (uses root Dockerfile)
docker-build:
	@echo "Building Docker image $(IMAGE_NAME)..."
	docker build -t $(IMAGE_NAME) -f ../Dockerfile ..

# Generate configuration (Docker Compose or Kubernetes)
generate: $(SCHEMA)
	@echo "Generating $(PLATFORM) configuration..."
	@mkdir -p $(OUTPUT_DIR)
	cargo run --release -- loadtest generate \
		--platform $(PLATFORM) \
		--source $(SOURCE) \
		--preset $(PRESET) \
		--schema $(SCHEMA) \
		--output-dir $(OUTPUT_DIR) \
		$(if $(WORKERS),--workers $(WORKERS)) \
		$(if $(ROW_COUNT),--row-count $(ROW_COUNT))

# Stop and remove containers
down:
	@echo "Stopping containers..."
	@-cd $(OUTPUT_DIR) && timeout 30 docker-compose -f docker-compose.loadtest.yml down -v --remove-orphans 2>/dev/null || true
	@echo "Containers stopped."

# Run the load test with monitoring for early failure detection
run: docker-build generate
	@echo "=== Starting load test ==="
	@echo "Source: $(SOURCE)"
	@echo "Containers: populate (x$(WORKERS)), sync, verify (x$(WORKERS)), aggregator"
	@echo "Timeout: $(TIMEOUT)s, Poll interval: $(POLL_INTERVAL)s"
	@echo ""
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml up -d --build
	@echo "Containers started. Monitoring for completion or failure..."
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

# CI target - delegates to Python script for JSON metrics output
# Usage: make run-ci SOURCE=kafka ROW_COUNT=100 TIMEOUT=300
run-ci:
	python3 ./scripts/run_ci.py \
		--source $(SOURCE) \
		--preset $(PRESET) \
		--row-count $(ROW_COUNT) \
		--workers $(WORKERS) \
		--timeout $(TIMEOUT)

# Wait for completion with early failure detection
# This monitors containers and exits immediately when any container fails
#
# Exit codes:
#   0 = All verify containers completed successfully
#   1 = Timeout reached (shows which containers were still running)
#   2 = Container failed (non-zero exit code)
#   3 = Verification mismatch (container exited 0 but reported mismatches)
wait-for-completion:
	@cd $(OUTPUT_DIR) && \
	start_time=$$(date +%s); \
	while true; do \
		elapsed=$$(( $$(date +%s) - start_time )); \
		\
		ps_output=$$(docker-compose -f docker-compose.loadtest.yml ps -a 2>/dev/null); \
		\
		populate_done=$$(echo "$$ps_output" | grep -E "output-populate-[0-9]+-1" | grep -c "Exited (0)" 2>/dev/null); \
		populate_done=$${populate_done:-0}; \
		sync_done=$$(echo "$$ps_output" | grep -E "output-(sync|incremental-sync)-1" | grep -c "Exited (0)" 2>/dev/null); \
		sync_done=$${sync_done:-0}; \
		verify_done=$$(echo "$$ps_output" | grep -E "output-verify-[0-9]+-1" | grep -c "Exited (0)" 2>/dev/null); \
		verify_done=$${verify_done:-0}; \
		\
		failed=$$(echo "$$ps_output" | grep -E "output-(schema-init|full-sync-setup|populate|sync|incremental-sync|verify)-[0-9]*-?1" | grep -E "Exited \([1-9]" | awk '{print $$1}' | head -5); \
		if [ -n "$$failed" ]; then \
			echo ""; \
			echo ""; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  FAILED: Container(s) exited with non-zero status                ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Failed containers:"; \
			echo "$$failed" | sed 's/^/  - /'; \
			echo ""; \
			for container in $$failed; do \
				echo "═══════════════════════════════════════════════════════════════════"; \
				echo "Logs from: $$container"; \
				echo "═══════════════════════════════════════════════════════════════════"; \
				docker logs "$$container" 2>&1 | tail -80 || echo "(could not retrieve logs)"; \
				echo ""; \
			done; \
			echo "Hint: Run 'make logs' or 'docker logs <container>' for full logs"; \
			docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null || true; \
			exit 2; \
		fi; \
		\
		printf "\r[%3ds] Populate: %s/$(WORKERS), Sync: %s/1, Verify: %s/$(WORKERS)" "$$elapsed" "$$populate_done" "$$sync_done" "$$verify_done"; \
		\
		if [ "$$sync_done" -ge 1 ] && [ "$$verify_done" -ge $(WORKERS) ]; then \
			echo ""; \
			echo ""; \
			verify_logs=$$(docker-compose -f docker-compose.loadtest.yml logs verify-1 verify-2 2>&1); \
			if echo "$$verify_logs" | grep -qE "[1-9][0-9]* mismatched"; then \
				echo "╔══════════════════════════════════════════════════════════════════╗"; \
				echo "║  VERIFICATION FAILED: Data mismatches detected                   ║"; \
				echo "╚══════════════════════════════════════════════════════════════════╝"; \
				echo ""; \
				echo "Verification results:"; \
				echo "$$verify_logs" | grep -E "Table|matched|mismatched|missing|successfully|Error" | sed 's/^/  /'; \
				echo ""; \
				echo "Hint: Check sync logic or schema definitions for data type issues"; \
				docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null || true; \
				exit 3; \
			fi; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  SUCCESS: All tables verified successfully                       ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Verification summary:"; \
			echo "$$verify_logs" | grep -E "Table|rows verified|successfully" | sed 's/^/  /' | tail -20; \
			echo ""; \
			docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null || true; \
			exit 0; \
		fi; \
		\
		if [ $$elapsed -ge $(TIMEOUT) ]; then \
			echo ""; \
			echo ""; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  TIMEOUT: Test did not complete within $(TIMEOUT) seconds               ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Progress at timeout:"; \
			echo "  Populate: $$populate_done/$(WORKERS) completed"; \
			echo "  Sync:     $$sync_done/1 completed"; \
			echo "  Verify:   $$verify_done/$(WORKERS) completed"; \
			echo ""; \
			if [ "$$populate_done" -lt "$(WORKERS)" ]; then \
				echo "BLOCKED AT: Populate stage - data generation not complete"; \
				echo ""; \
				echo "Populate container logs:"; \
				for i in 1 2; do \
					echo ""; \
					echo "─── populate-$$i (last 30 lines) ───"; \
					docker-compose -f docker-compose.loadtest.yml logs --tail=30 populate-$$i 2>/dev/null || echo "(no logs)"; \
				done; \
			elif [ "$$sync_done" -lt "1" ]; then \
				echo "BLOCKED AT: Sync stage - data synchronization not complete"; \
				echo ""; \
				echo "─── sync / incremental-sync (last 50 lines) ───"; \
				docker-compose -f docker-compose.loadtest.yml logs --tail=50 sync incremental-sync 2>/dev/null || echo "(no logs)"; \
			else \
				echo "BLOCKED AT: Verify stage - verification not complete"; \
				echo ""; \
				echo "Verify container logs:"; \
				for i in 1 2; do \
					echo ""; \
					echo "─── verify-$$i (last 30 lines) ───"; \
					docker-compose -f docker-compose.loadtest.yml logs --tail=30 verify-$$i 2>/dev/null || echo "(no logs)"; \
				done; \
			fi; \
			echo ""; \
			echo "Full container status:"; \
			docker-compose -f docker-compose.loadtest.yml ps -a 2>/dev/null || true; \
			echo ""; \
			docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null || true; \
			exit 1; \
		fi; \
		\
		sleep $(POLL_INTERVAL); \
	done

# Run in detached mode without monitoring
run-detached: docker-build generate
	@echo "Starting load test in background..."
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml up -d --build
	@echo "Use 'make logs' to view output or 'make status' to check progress"

# Show logs (all containers, follow mode)
logs:
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs -f

# Show verification results (matched/mismatched/missing counts)
logs-verify:
	@echo "=== Verification Results ==="
	@cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs verify-1 verify-2 2>&1 | \
		grep -E "(matched|missing|mismatched|successfully|MISMATCH|Table:)" | tail -30
	@echo ""
	@echo "For full verify logs: cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs verify-1 verify-2"

# Show sync container logs
logs-sync:
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs sync

# Show populate container logs
logs-populate:
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs populate-1 populate-2

# Show status
status:
	@echo "Container status:"
	@cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml ps -a 2>/dev/null || echo "(no containers)"
	@echo ""
	@echo "Recent logs (last 10 lines per container):"
	@cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs --tail=10 2>/dev/null || true

# Clean up
clean: down
	@echo "Cleaning up..."
	-docker rm -f $$(docker ps -aq --filter "label=com.surreal-loadtest") 2>/dev/null || true
	-docker volume prune -f --filter "label=com.surreal-loadtest" 2>/dev/null || true
	@echo "Cleaning up old images..."
	-docker image prune -f --filter "dangling=true" 2>/dev/null || true
	-docker images surreal-sync --format "{{.ID}}" | tail -n +2 | xargs -r docker rmi -f 2>/dev/null || true
	rm -rf $(OUTPUT_DIR)/docker-compose.loadtest.yml
	rm -rf $(OUTPUT_DIR)/kubernetes/
	@echo "Done."

# Verify results (for CI/CD)
verify:
	@echo "Checking aggregator result..."
	@curl -sf http://localhost:9090/status || (echo "Aggregator not responding" && exit 1)

# Integration test: Docker Compose with --dry-run
# Verifies the generated config runs correctly without external databases
test-dry-run-docker-compose: docker-build
	@echo "=== Testing Docker Compose with --dry-run ==="
	@echo "1. Building binary..."
	cd .. && cargo build --release
	@echo "2. Generating Docker Compose config with --dry-run..."
	@mkdir -p $(OUTPUT_DIR)
	../target/release/surreal-sync loadtest generate \
		--platform docker-compose \
		--source mysql \
		--preset small \
		--schema $(SCHEMA) \
		--output-dir $(OUTPUT_DIR) \
		--dry-run
	@echo "3. Starting containers..."
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml up -d
	@echo "4. Monitoring for completion (timeout 120s)..."
	@$(MAKE) -s wait-for-completion TIMEOUT=120 POLL_INTERVAL=3 2>&1 || \
		(echo "Test failed - see logs above" && exit 1)
	@echo "=== Docker Compose dry-run test PASSED ==="

# Integration test: Kubernetes with --dry-run
# Requires kubectl configured with a local cluster (e.g., kind, minikube)
test-dry-run-kubernetes: docker-build
	@echo "=== Testing Kubernetes with --dry-run ==="
	@echo "1. Building binary..."
	cd .. && cargo build --release
	@echo "2. Generating Kubernetes config with --dry-run..."
	@mkdir -p $(OUTPUT_DIR)/kubernetes
	../target/release/surreal-sync loadtest generate \
		--platform kubernetes \
		--source mysql \
		--preset small \
		--schema $(SCHEMA) \
		--output-dir $(OUTPUT_DIR) \
		--dry-run
	@echo "2.5. Loading image into kind cluster (if available)..."
	-kind load docker-image $(IMAGE_NAME) --name $(KIND_CLUSTER) 2>/dev/null || echo "kind not available, assuming image in registry"
	@echo "3. Applying Kubernetes resources..."
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/
	@echo "4. Waiting for populate job to complete (timeout 120s)..."
	kubectl wait --for=condition=complete job/loadtest-populate -n loadtest --timeout=120s || (echo "Populate job failed or timed out" && kubectl logs job/loadtest-populate -n loadtest && exit 1)
	@echo "5. Verifying populate job logs contain [DRY-RUN]..."
	@kubectl logs job/loadtest-populate -n loadtest 2>&1 | grep -q "\[DRY-RUN\]" && echo "✓ populate job dry-run OK" || (echo "✗ populate job missing [DRY-RUN]" && exit 1)
	@echo "6. Waiting for sync job to complete (timeout 120s)..."
	kubectl wait --for=condition=complete job/loadtest-sync -n loadtest --timeout=120s || (echo "Sync job failed or timed out" && kubectl logs job/loadtest-sync -n loadtest && exit 1)
	@echo "7. Verifying sync job logs contain [DRY-RUN]..."
	@kubectl logs job/loadtest-sync -n loadtest 2>&1 | grep -q "\[DRY-RUN\]" && echo "✓ sync job dry-run OK" || (echo "✗ sync job missing [DRY-RUN]" && exit 1)
	@echo "8. Waiting for verify job to complete (timeout 120s)..."
	kubectl wait --for=condition=complete job/loadtest-verify -n loadtest --timeout=120s || (echo "Verify job failed or timed out" && kubectl logs job/loadtest-verify -n loadtest && exit 1)
	@echo "9. Verifying verify job logs contain [DRY-RUN]..."
	@kubectl logs job/loadtest-verify -n loadtest 2>&1 | grep -q "\[DRY-RUN\]" && echo "✓ verify job dry-run OK" || (echo "✗ verify job missing [DRY-RUN]" && exit 1)
	@echo "10. Cleaning up Kubernetes resources..."
	-kubectl delete -f $(OUTPUT_DIR)/kubernetes/ --ignore-not-found 2>/dev/null
	-kubectl delete namespace loadtest --ignore-not-found 2>/dev/null
	@echo "=== Kubernetes dry-run test PASSED ==="

# Helper to start containers in detached mode
wait-for-start:
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml up -d --build
	@echo "Containers started. Monitoring..."

# =============================================================================
# Kind cluster management
# =============================================================================

kind-create:
	@echo "Creating kind cluster '$(KIND_CLUSTER)'..."
	kind create cluster --name $(KIND_CLUSTER) 2>/dev/null || echo "Cluster '$(KIND_CLUSTER)' already exists"

kind-delete:
	@echo "Deleting kind cluster '$(KIND_CLUSTER)'..."
	kind delete cluster --name $(KIND_CLUSTER)

kind-load: docker-build
	@echo "Loading images into kind cluster '$(KIND_CLUSTER)'..."
	@echo "Loading surreal-sync image..."
	kind load docker-image $(IMAGE_NAME) --name $(KIND_CLUSTER)
	@echo "Loading kubectl image for init containers..."
	docker pull bitnami/kubectl:latest 2>/dev/null || true
	kind load docker-image bitnami/kubectl:latest --name $(KIND_CLUSTER) 2>/dev/null || true
	@echo "Loading busybox image for init containers..."
	docker pull busybox:latest 2>/dev/null || true
	kind load docker-image busybox:latest --name $(KIND_CLUSTER) 2>/dev/null || true
	@echo "Loading SurrealDB image..."
	docker pull surrealdb/surrealdb:latest 2>/dev/null || true
	kind load docker-image surrealdb/surrealdb:latest --name $(KIND_CLUSTER) 2>/dev/null || true
	@echo "Loading source-specific image for $(SOURCE)..."
	@$(MAKE) -s kind-load-source-image SOURCE=$(SOURCE)

# Load source-specific database image
kind-load-source-image:
ifeq ($(SOURCE),mysql)
	docker pull mysql:8.0 2>/dev/null || true
	kind load docker-image mysql:8.0 --name $(KIND_CLUSTER) 2>/dev/null || true
else ifeq ($(SOURCE),postgresql)
	docker pull postgres:16 2>/dev/null || true
	kind load docker-image postgres:16 --name $(KIND_CLUSTER) 2>/dev/null || true
else ifeq ($(SOURCE),postgresql-logical)
	docker pull debezium/postgres:16 2>/dev/null || true
	kind load docker-image debezium/postgres:16 --name $(KIND_CLUSTER) 2>/dev/null || true
else ifeq ($(SOURCE),mongodb)
	docker pull mongo:7 2>/dev/null || true
	kind load docker-image mongo:7 --name $(KIND_CLUSTER) 2>/dev/null || true
else ifeq ($(SOURCE),neo4j)
	docker pull neo4j:5 2>/dev/null || true
	kind load docker-image neo4j:5 --name $(KIND_CLUSTER) 2>/dev/null || true
else ifeq ($(SOURCE),kafka)
	docker pull apache/kafka:latest 2>/dev/null || true
	kind load docker-image apache/kafka:latest --name $(KIND_CLUSTER) 2>/dev/null || true
endif
	@echo "Source image loaded for $(SOURCE)"

# =============================================================================
# Kubernetes Load Testing (kind)
# =============================================================================
#
# Usage: make k8s-run SOURCE=mysql TIMEOUT=180
#
# Key Differences from Docker Compose:
# | Aspect           | Docker Compose                  | Kubernetes                            |
# |------------------|---------------------------------|---------------------------------------|
# | Dependency       | depends_on: condition           | Init containers with kubectl wait     |
# | Schema mount     | ./config:/config:ro             | ConfigMap with embedded content       |
# | Job completion   | service_completed_successfully  | kubectl wait --for=condition=complete |
# | Database health  | service_healthy                 | Init container with health script     |
# | RBAC             | N/A                             | ServiceAccount + Role + RoleBinding   |
# | Image            | Local docker build              | kind load docker-image                |
#
# =============================================================================

k8s-run: kind-create kind-load k8s-clean
	@echo "=== Starting Kubernetes load test ==="
	@echo "Source: $(SOURCE)"
	@echo "Timeout: $(TIMEOUT)s"
	@echo ""
	$(MAKE) generate SOURCE=$(SOURCE) PLATFORM=kubernetes
	@echo "Creating namespace first..."
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/namespace.yaml
	@echo "Waiting for namespace to be active..."
	@kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/loadtest --timeout=30s
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/
	@echo "Resources applied. Monitoring for completion..."
	@$(MAKE) -s k8s-wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

# Wait for Kubernetes jobs to complete with early failure detection
k8s-wait-for-completion:
	@start_time=$$(date +%s); \
	while true; do \
		elapsed=$$(( $$(date +%s) - start_time )); \
		\
		jobs_json=$$(kubectl get jobs -n loadtest -o json 2>/dev/null); \
		\
		populate_succeeded=$$(echo "$$jobs_json" | jq '[.items[] | select(.metadata.name | startswith("loadtest-populate")) | select(.status.succeeded == 1)] | length' 2>/dev/null || echo 0); \
		populate_total=$$(echo "$$jobs_json" | jq '[.items[] | select(.metadata.name | startswith("loadtest-populate"))] | length' 2>/dev/null || echo 0); \
		sync_succeeded=$$(echo "$$jobs_json" | jq '[.items[] | select(.metadata.name | startswith("loadtest-sync")) | select(.status.succeeded == 1)] | length' 2>/dev/null || echo 0); \
		sync_total=$$(echo "$$jobs_json" | jq '[.items[] | select(.metadata.name | startswith("loadtest-sync"))] | length' 2>/dev/null || echo 0); \
		verify_succeeded=$$(echo "$$jobs_json" | jq '[.items[] | select(.metadata.name | startswith("loadtest-verify")) | select(.status.succeeded == 1)] | length' 2>/dev/null || echo 0); \
		verify_total=$$(echo "$$jobs_json" | jq '[.items[] | select(.metadata.name | startswith("loadtest-verify"))] | length' 2>/dev/null || echo 0); \
		\
		failed_jobs=$$(echo "$$jobs_json" | jq -r '.items[] | select(.status.failed >= 1) | .metadata.name' 2>/dev/null); \
		if [ -n "$$failed_jobs" ]; then \
			echo ""; \
			echo ""; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  FAILED: Kubernetes job(s) failed                                ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Failed jobs:"; \
			echo "$$failed_jobs" | sed 's/^/  - /'; \
			echo ""; \
			for job in $$failed_jobs; do \
				echo "═══════════════════════════════════════════════════════════════════"; \
				echo "Logs from: $$job"; \
				echo "═══════════════════════════════════════════════════════════════════"; \
				kubectl logs job/$$job -n loadtest 2>&1 | tail -80 || echo "(could not retrieve logs)"; \
				echo ""; \
			done; \
			echo ""; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  DEBUG: Resources preserved for investigation                    ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Useful commands:"; \
			echo "  kubectl get pods -n loadtest              # List all pods"; \
			echo "  kubectl get jobs -n loadtest              # List all jobs"; \
			echo "  kubectl logs job/<job-name> -n loadtest   # Get job logs"; \
			echo "  kubectl describe pod <pod-name> -n loadtest # Get pod details"; \
			echo "  kubectl exec -it <pod-name> -n loadtest -- sh  # Shell into pod"; \
			echo ""; \
			echo "When done investigating, run:"; \
			echo "  make k8s-clean"; \
			echo ""; \
			exit 2; \
		fi; \
		\
		printf "\r[%3ds] Populate: %s/%s, Sync: %s/%s, Verify: %s/%s" "$$elapsed" "$$populate_succeeded" "$$populate_total" "$$sync_succeeded" "$$sync_total" "$$verify_succeeded" "$$verify_total"; \
		\
		if [ "$$verify_total" -gt 0 ] && [ "$$verify_succeeded" -ge "$$verify_total" ]; then \
			echo ""; \
			echo ""; \
			verify_logs=$$(kubectl logs -l app=loadtest-verify -n loadtest 2>&1); \
			if echo "$$verify_logs" | grep -qE "[1-9][0-9]* mismatched"; then \
				echo "╔══════════════════════════════════════════════════════════════════╗"; \
				echo "║  VERIFICATION FAILED: Data mismatches detected                   ║"; \
				echo "╚══════════════════════════════════════════════════════════════════╝"; \
				echo ""; \
				echo "Verification results:"; \
				echo "$$verify_logs" | grep -E "Table|matched|mismatched|missing|successfully|Error" | sed 's/^/  /'; \
				echo ""; \
				echo "╔══════════════════════════════════════════════════════════════════╗"; \
				echo "║  DEBUG: Resources preserved for investigation                    ║"; \
				echo "╚══════════════════════════════════════════════════════════════════╝"; \
				echo ""; \
				echo "Useful commands:"; \
				echo "  kubectl get pods -n loadtest              # List all pods"; \
				echo "  kubectl exec -it surrealdb-0 -n loadtest -- /surreal sql  # Query SurrealDB"; \
				echo "  kubectl logs job/verify-<table> -n loadtest  # Verify logs"; \
				echo ""; \
				echo "When done investigating, run:"; \
				echo "  make k8s-clean"; \
				echo ""; \
				exit 3; \
			fi; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  SUCCESS: All tables verified successfully                       ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Verification summary:"; \
			echo "$$verify_logs" | grep -E "Table|rows verified|successfully" | sed 's/^/  /' | tail -20; \
			echo ""; \
			$(MAKE) k8s-clean 2>/dev/null || true; \
			exit 0; \
		fi; \
		\
		if [ $$elapsed -ge $(TIMEOUT) ]; then \
			echo ""; \
			echo ""; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  TIMEOUT: Test did not complete within $(TIMEOUT) seconds               ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Progress at timeout:"; \
			echo "  Populate: $$populate_succeeded/$$populate_total completed"; \
			echo "  Sync:     $$sync_succeeded/$$sync_total completed"; \
			echo "  Verify:   $$verify_succeeded/$$verify_total completed"; \
			echo ""; \
			if [ "$$populate_succeeded" -lt "$$populate_total" ]; then \
				echo "BLOCKED AT: Populate stage"; \
				kubectl logs -l app=loadtest-populate -n loadtest --tail=30 2>/dev/null || echo "(no logs)"; \
			elif [ "$$sync_succeeded" -lt "$$sync_total" ]; then \
				echo "BLOCKED AT: Sync stage"; \
				kubectl logs -l app=loadtest-sync -n loadtest --tail=50 2>/dev/null || echo "(no logs)"; \
			else \
				echo "BLOCKED AT: Verify stage"; \
				kubectl logs -l app=loadtest-verify -n loadtest --tail=30 2>/dev/null || echo "(no logs)"; \
			fi; \
			echo ""; \
			echo "Full job status:"; \
			kubectl get jobs -n loadtest 2>/dev/null || true; \
			echo ""; \
			echo "╔══════════════════════════════════════════════════════════════════╗"; \
			echo "║  DEBUG: Resources preserved for investigation                    ║"; \
			echo "╚══════════════════════════════════════════════════════════════════╝"; \
			echo ""; \
			echo "Useful commands:"; \
			echo "  kubectl get pods -n loadtest              # List all pods"; \
			echo "  kubectl get jobs -n loadtest              # List all jobs"; \
			echo "  kubectl describe pod <pod-name> -n loadtest # Get pod details"; \
			echo "  kubectl logs <pod-name> -n loadtest       # Get pod logs"; \
			echo "  kubectl logs <pod-name> -c init-wait-* -n loadtest  # Init container logs"; \
			echo ""; \
			echo "When done investigating, run:"; \
			echo "  make k8s-clean"; \
			echo ""; \
			exit 1; \
		fi; \
		\
		sleep $(POLL_INTERVAL); \
	done

# Kubernetes cleanup
k8s-clean:
	@echo "Cleaning up Kubernetes resources..."
	-kubectl delete -f $(OUTPUT_DIR)/kubernetes/ --ignore-not-found 2>/dev/null
	-kubectl delete namespace loadtest --ignore-not-found 2>/dev/null
	@echo "Done."

# =============================================================================
# Aggregator verification
# =============================================================================

verify-aggregator:
	@echo "Checking aggregator status..."
	@curl -sf http://localhost:9090/status || (echo "Aggregator not responding" && exit 1)
	@echo "Aggregator response:"
	@curl -s http://localhost:9090/status | jq . 2>/dev/null || curl -s http://localhost:9090/status
	@echo ""
	@echo "Verifying no container failures..."
	@curl -sf http://localhost:9090/status | jq -e '.workers.failed == 0 or .failed_workers == 0 or .status == "success"' >/dev/null 2>&1 && echo "✓ No failures" || (echo "✗ Containers failed!" && exit 1)
	@echo "Aggregator verification PASSED"

# =============================================================================
# CI integration: test all sources
# =============================================================================

test-all-sources:
	@echo "=== Testing all source types ==="
	@echo "--- MySQL ---"
	$(MAKE) run SOURCE=mysql TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "--- PostgreSQL ---"
	$(MAKE) run SOURCE=postgresql TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "--- MongoDB ---"
	$(MAKE) run SOURCE=mongodb TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "--- Neo4j ---"
	$(MAKE) run SOURCE=neo4j TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "=== All source tests PASSED ==="

# =============================================================================
# Script testing (no external dependencies)
# =============================================================================

# Test CI scripts without external dependencies (no docker, no cargo)
# Uses sample JSON files to verify script logic
test-scripts:
	@echo "Running script unit tests..."
	./scripts/test-scripts.sh

# Clean up all loadtest resources (containers, volumes, networks)
# Use this to clean up after a failed run-ci test
clean-all:
	@echo "Cleaning up all loadtest resources..."
	./scripts/cleanup.sh --force
