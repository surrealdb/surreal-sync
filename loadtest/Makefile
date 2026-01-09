# Makefile for distributed load testing
#
# Usage:
#   make docker-build    # Build the surreal-sync Docker image
#   make generate        # Generate Docker Compose configuration
#   make run             # Run the load test with monitoring
#   make clean           # Clean up containers and volumes
#
# The test pipeline runs: populate -> sync -> verify
# Each stage must complete successfully before the next starts.

.PHONY: docker-build generate run run-detached clean logs status help \
	test-dry-run-docker-compose test-dry-run-kubernetes \
	run-mysql run-postgresql run-mongodb run-neo4j run-kafka run-csv run-jsonl \
	kind-create kind-delete kind-load k8s-run-mysql k8s-run-postgresql k8s-run-mongodb k8s-run-neo4j \
	verify-aggregator test-all-sources down wait-for-completion

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
	@echo "Targets:"
	@echo "  docker-build   Build the surreal-sync Docker image"
	@echo "  generate       Generate Docker Compose configuration"
	@echo "  run            Run the load test with monitoring (detects failures early)"
	@echo "  run-detached   Run in background without monitoring"
	@echo "  down           Stop and remove all containers"
	@echo "  clean          Clean up containers, volumes, and generated files"
	@echo "  logs           Show logs from containers"
	@echo "  status         Show container status"
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
	@echo "  make run                              # Run with defaults"
	@echo "  make run SOURCE=postgresql WORKERS=4 # PostgreSQL with 4 containers"
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

# Wait for completion with early failure detection
# This monitors containers and exits immediately when any container fails
wait-for-completion:
	@start_time=$$(date +%s); \
	while true; do \
		elapsed=$$(( $$(date +%s) - start_time )); \
		\
		if [ $$elapsed -ge $(TIMEOUT) ]; then \
			echo ""; \
			echo "=== TIMEOUT after $(TIMEOUT)s ==="; \
			echo "Dumping logs from all containers:"; \
			cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs 2>/dev/null; \
			cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null; \
			exit 1; \
		fi; \
		\
		ps_output=$$(cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml ps -a 2>/dev/null); \
		\
		failed=$$(echo "$$ps_output" | grep -E "Exit [1-9]|Exited \([1-9]" | awk '{print $$1}' | head -5); \
		if [ -n "$$failed" ]; then \
			echo ""; \
			echo "=== FAILED containers detected ==="; \
			echo "$$failed"; \
			echo ""; \
			echo "=== Failed container logs ==="; \
			for container in $$failed; do \
				echo "--- $$container ---"; \
				docker logs "$$container" 2>&1 | tail -50 || true; \
				echo ""; \
			done; \
			cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null; \
			exit 1; \
		fi; \
		\
		completed=$$(echo "$$ps_output" | grep -cE "Exit 0|Exited \(0\)" 2>/dev/null || echo "0"); \
		running=$$(echo "$$ps_output" | grep -cE "Up|running" 2>/dev/null || echo "0"); \
		\
		printf "\r[%3ds] Running: %s, Completed: %s" "$$elapsed" "$$running" "$$completed"; \
		\
		sync_done=$$(echo "$$ps_output" | grep sync | grep -cE "Exit 0|Exited \(0\)" 2>/dev/null || echo "0"); \
		if [ "$$sync_done" = "1" ]; then \
			verify_done=$$(echo "$$ps_output" | grep verify | grep -cE "Exit 0|Exited \(0\)" 2>/dev/null || echo "0"); \
			if [ "$$verify_done" -ge "$(WORKERS)" ]; then \
				echo ""; \
				echo ""; \
				echo "=== SUCCESS - All containers completed ==="; \
				echo ""; \
				echo "=== Summary logs ==="; \
				cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs --tail=20 2>/dev/null; \
				cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml down -v 2>/dev/null; \
				exit 0; \
			fi; \
		fi; \
		\
		sleep $(POLL_INTERVAL); \
	done

# Run in detached mode without monitoring
run-detached: docker-build generate
	@echo "Starting load test in background..."
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml up -d --build
	@echo "Use 'make logs' to view output or 'make status' to check progress"

# Show logs
logs:
	cd $(OUTPUT_DIR) && docker-compose -f docker-compose.loadtest.yml logs -f

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

# =============================================================================
# Source-specific Docker Compose targets (real databases, no dry-run)
# =============================================================================

run-mysql: docker-build
	@echo "Running load test with MySQL..."
	$(MAKE) generate SOURCE=mysql PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

run-postgresql: docker-build
	@echo "Running load test with PostgreSQL..."
	$(MAKE) generate SOURCE=postgresql PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

run-mongodb: docker-build
	@echo "Running load test with MongoDB..."
	$(MAKE) generate SOURCE=mongodb PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

run-neo4j: docker-build
	@echo "Running load test with Neo4j..."
	$(MAKE) generate SOURCE=neo4j PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

run-kafka: docker-build
	@echo "Running load test with Kafka..."
	$(MAKE) generate SOURCE=kafka PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

run-csv: docker-build
	@echo "Running load test with CSV..."
	$(MAKE) generate SOURCE=csv PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

run-jsonl: docker-build
	@echo "Running load test with JSONL..."
	$(MAKE) generate SOURCE=jsonl PLATFORM=docker-compose
	$(MAKE) wait-for-start
	@$(MAKE) -s wait-for-completion TIMEOUT=$(TIMEOUT) POLL_INTERVAL=$(POLL_INTERVAL)

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
	@echo "Loading image into kind cluster '$(KIND_CLUSTER)'..."
	kind load docker-image $(IMAGE_NAME) --name $(KIND_CLUSTER)

# =============================================================================
# Kubernetes source-specific targets (requires kind or other k8s cluster)
# =============================================================================

k8s-run-mysql: kind-load
	@echo "Running Kubernetes load test with MySQL..."
	$(MAKE) generate SOURCE=mysql PLATFORM=kubernetes
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/
	@echo "Watching jobs... (Ctrl+C to stop)"
	kubectl get jobs -n loadtest -w

k8s-run-postgresql: kind-load
	@echo "Running Kubernetes load test with PostgreSQL..."
	$(MAKE) generate SOURCE=postgresql PLATFORM=kubernetes
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/
	@echo "Watching jobs... (Ctrl+C to stop)"
	kubectl get jobs -n loadtest -w

k8s-run-mongodb: kind-load
	@echo "Running Kubernetes load test with MongoDB..."
	$(MAKE) generate SOURCE=mongodb PLATFORM=kubernetes
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/
	@echo "Watching jobs... (Ctrl+C to stop)"
	kubectl get jobs -n loadtest -w

k8s-run-neo4j: kind-load
	@echo "Running Kubernetes load test with Neo4j..."
	$(MAKE) generate SOURCE=neo4j PLATFORM=kubernetes
	kubectl apply -f $(OUTPUT_DIR)/kubernetes/
	@echo "Watching jobs... (Ctrl+C to stop)"
	kubectl get jobs -n loadtest -w

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
	$(MAKE) run-mysql TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "--- PostgreSQL ---"
	$(MAKE) run-postgresql TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "--- MongoDB ---"
	$(MAKE) run-mongodb TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "--- Neo4j ---"
	$(MAKE) run-neo4j TIMEOUT=$(TIMEOUT)
	$(MAKE) verify-aggregator
	$(MAKE) clean
	@echo "=== All source tests PASSED ==="
