PYTHON ?= python
NPM ?= npm
CURSOR_CMD ?= /Applications/Cursor.app/Contents/Resources/app/bin/cursor
VSCODE_CMD ?= code
IDE_CMD ?= $(CURSOR_CMD)
NPM_REGISTRY ?= https://registry.npmjs.org
RUNNER := data_manipulation/local_test_runner.py
EXT_DIR := vscode-extension
EXT_NAME := sql-pyspark-test-runner
EXT_VERSION := 0.0.1
EXT_VSIX := $(EXT_DIR)/$(EXT_NAME)-$(EXT_VERSION).vsix
RAW_TARGET := $(word 2,$(MAKECMDGOALS))
PROBLEM_FROM_GOAL := $(patsubst %.test.json,%,$(patsubst %.sql,%,$(patsubst %.py,%,$(RAW_TARGET))))

.PHONY: test test-all clean sqlpyspark-test sqlpyspark-test-all sqlpyspark-test-case ext-deps ext-build ext-package ext-install

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
	rm -rf data_manipulation/**/__pycache__

test-all:
	$(PYTHON) "$(RUNNER)" --all

sqlpyspark-test:
	@if [ -z "$(PROBLEM)" ]; then \
		echo "Usage: make sqlpyspark-test PROBLEM=leetcode_hard/active_user_retention"; \
		exit 1; \
	fi
	$(PYTHON) "$(RUNNER)" --problem "$(PROBLEM)"

sqlpyspark-test-all:
	$(MAKE) test-all

sqlpyspark-test-case:
	@if [ -z "$(CASE)" ]; then \
		echo "Usage: make sqlpyspark-test-case CASE=leetcode_hard/active_user_retention.test.json"; \
		exit 1; \
	fi
	$(PYTHON) "$(RUNNER)" --case "$(CASE)"

ext-deps:
	$(NPM) --prefix "$(EXT_DIR)" --registry "$(NPM_REGISTRY)" install

ext-build: ext-deps
	$(NPM) --prefix "$(EXT_DIR)" run compile

ext-package: ext-build
	$(NPM) --prefix "$(EXT_DIR)" run package

ext-install: ext-package
	$(IDE_CMD) --install-extension "$(EXT_VSIX)" --force

# Swallow the extra positional goal in: make test <filename>
%:
	@:
