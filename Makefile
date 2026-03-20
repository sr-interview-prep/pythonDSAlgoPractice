PYTHON ?= python
RUNNER := sql_pyspark/local_test_runner.py
RAW_TARGET := $(word 2,$(MAKECMDGOALS))
PROBLEM_FROM_GOAL := $(patsubst %.test.json,%,$(patsubst %.sql,%,$(patsubst %.py,%,$(RAW_TARGET))))

.PHONY: test clean sqlpyspark-test sqlpyspark-test-all sqlpyspark-test-case

test:
	@if [ -z "$(RAW_TARGET)" ]; then \
		echo "Usage: make test <fileName>"; \
		echo "Examples: make test active_user_retention"; \
		echo "          make test leetcode_hard/active_user_retention"; \
		echo "Also supported: .sql, .py, or .test.json suffix."; \
		exit 1; \
	fi
	$(PYTHON) "$(RUNNER)" --problem "$(PROBLEM_FROM_GOAL)"

clean:
	rm -rf "__pycache__" ".pytest_cache" "metastore_db" "spark-warehouse" "derby.log"
	rm -rf sql_pyspark/**/__pycache__

sqlpyspark-test:
	@if [ -z "$(PROBLEM)" ]; then \
		echo "Usage: make sqlpyspark-test PROBLEM=leetcode_hard/active_user_retention"; \
		exit 1; \
	fi
	$(PYTHON) "$(RUNNER)" --problem "$(PROBLEM)"

sqlpyspark-test-all:
	$(PYTHON) "$(RUNNER)" --all

sqlpyspark-test-case:
	@if [ -z "$(CASE)" ]; then \
		echo "Usage: make sqlpyspark-test-case CASE=leetcode_hard/active_user_retention.test.json"; \
		exit 1; \
	fi
	$(PYTHON) "$(RUNNER)" --case "$(CASE)"

# Swallow the extra positional goal in: make test <filename>
%:
	@:
