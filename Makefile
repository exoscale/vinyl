.PHONY: *
.DEFAULT_GOAL:=help

# Internal
ROOT_DIR=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
VERSION=$(shell cat $(ROOT_DIR)/VERSION)
CLJ=clojure

##@ Testing

test: ## Runs tests
	$(CLJ) -T:project test

##@ Dependencies

deps: ## Show deps tree
	$(CLJ) -Stree

check: ## Compile all namespaces to check for issues
	$(CLJ) -T:project check

merge-deps: ## Merge dependencies on all modules from :managed-deps
	$(CLJ) -T:project merge-deps

merge-aliases: ## Merge aliases on all modules from :managed-aliases
	$(CLJ) -T:project merge-aliases

##@ Misc.

lint: ## runs linting on all modules
	$(CLJ) -T:project lint

format: ## Format according to linter rules
	$(CLJ) -T:project format-check

format-fix: ## Fix formatting errors found
	$(CLJ) -T:project format-fix

outdated: ## run antq (aka 'ancient') task on all modules
	$(CLJ) -T:project outdated

clean: ## Clean module target dirs
	$(CLJ) -T:project clean

install: ## Install all modules to local maven repo
	$(CLJ) -T:project install

version: ## Output project version
	@$(CLJ) -T:project version

repl: ## Launch repl
	@clj -A:test

##@ CI/CD tasks

release: ## Release jar modules & tag versions
	git config --global --add safe.directory '*'
	$(CLJ) -T:project release

build: check ## An alias for check

##@ Helpers

.SILENT: info
info: ## Show repo information
	@$(CLJ) -T:project info

help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
