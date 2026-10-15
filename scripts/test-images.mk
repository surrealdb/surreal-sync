# Load canonical test image tags from scripts/test-images.env.
MYSQL_BINLOG_IMAGE := $(shell grep -E '^MYSQL_BINLOG_IMAGE=' scripts/test-images.env | cut -d= -f2-)
MARIADB_BINLOG_IMAGE := $(shell grep -E '^MARIADB_BINLOG_IMAGE=' scripts/test-images.env | cut -d= -f2-)
MSSQL_IMAGE := $(shell grep -E '^MSSQL_IMAGE=' scripts/test-images.env | cut -d= -f2-)
BIGQUERY_EMULATOR_IMAGE := $(shell grep -E '^BIGQUERY_EMULATOR_IMAGE=' scripts/test-images.env | cut -d= -f2-)
export MYSQL_BINLOG_IMAGE MARIADB_BINLOG_IMAGE MSSQL_IMAGE BIGQUERY_EMULATOR_IMAGE

.PHONY: prepull-binlog-images prepull-mssql-image prepull-bigquery-image print-test-images

prepull-binlog-images:
	@echo "🐳 Pre-pulling MySQL/MariaDB binlog test images ($(MYSQL_BINLOG_IMAGE), $(MARIADB_BINLOG_IMAGE))..."
	docker pull $(MYSQL_BINLOG_IMAGE)
	docker pull $(MARIADB_BINLOG_IMAGE)
	@echo "✅ Binlog test images ready"

prepull-mssql-image:
	@echo "🐳 Pre-pulling SQL Server test image ($(MSSQL_IMAGE))..."
	docker pull $(MSSQL_IMAGE)
	@echo "✅ SQL Server test image ready"

prepull-bigquery-image:
	@echo "🐳 Pre-pulling BigQuery emulator test image ($(BIGQUERY_EMULATOR_IMAGE))..."
	docker pull $(BIGQUERY_EMULATOR_IMAGE)
	@echo "✅ BigQuery emulator image ready"

print-test-images:
	@echo MYSQL_BINLOG_IMAGE=$(MYSQL_BINLOG_IMAGE)
	@echo MARIADB_BINLOG_IMAGE=$(MARIADB_BINLOG_IMAGE)
	@echo MSSQL_IMAGE=$(MSSQL_IMAGE)
	@echo BIGQUERY_EMULATOR_IMAGE=$(BIGQUERY_EMULATOR_IMAGE)
