# Load canonical binlog test image tags from scripts/test-images.env.
MYSQL_BINLOG_IMAGE := $(shell grep -E '^MYSQL_BINLOG_IMAGE=' scripts/test-images.env | cut -d= -f2-)
MARIADB_BINLOG_IMAGE := $(shell grep -E '^MARIADB_BINLOG_IMAGE=' scripts/test-images.env | cut -d= -f2-)
export MYSQL_BINLOG_IMAGE MARIADB_BINLOG_IMAGE

.PHONY: prepull-binlog-images print-test-images

prepull-binlog-images:
	@echo "🐳 Pre-pulling MySQL/MariaDB binlog test images ($(MYSQL_BINLOG_IMAGE), $(MARIADB_BINLOG_IMAGE))..."
	docker pull $(MYSQL_BINLOG_IMAGE)
	docker pull $(MARIADB_BINLOG_IMAGE)
	@echo "✅ Binlog test images ready"

print-test-images:
	@echo MYSQL_BINLOG_IMAGE=$(MYSQL_BINLOG_IMAGE)
	@echo MARIADB_BINLOG_IMAGE=$(MARIADB_BINLOG_IMAGE)
