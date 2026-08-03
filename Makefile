REQUIRED_VARS := GOOGLE_SPANNER_DATABASE GOOGLE_SPANNER_INSTANCE GOOGLE_CLOUD_REGION GOOGLE_PROJECT

# Source the env file
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Check to make sure all the required env vars are set
$(foreach var,$(REQUIRED_VARS),\
    $(if $(strip $($(var))),,\
        $(error Environment variable $(var) is empty or not set!)\
    )\
)

default: help

# A target to specifically check for uv
check_uv:
	@command -v uv >/dev/null 2>&1 || (echo "🚨 ERROR: 'uv' not found. Please install it." && exit 1)
	@echo "✅ uv is installed and ready."

showenv: ## Print out our environment
	@echo "GOOGLE_SPANNER_DATABASE       -> $(GOOGLE_SPANNER_DATABASE)"
	@echo "GOOGLE_SPANNER_INSTANCE       -> $(GOOGLE_SPANNER_INSTANCE)"
	@echo "GOOGLE_CLOUD_REGION           -> $(GOOGLE_CLOUD_REGION)"
	@echo "GOOGLE_PROJECT                -> $(GOOGLE_PROJECT)"

##@ Utility
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

dbcreate: ## Create Database with DDL.sql schema
	@gcloud spanner databases create $(GOOGLE_SPANNER_DATABASE) --instance $(GOOGLE_SPANNER_INSTANCE) --ddl-file=DDL.sql

dbschema: ## Apply or update database schema from DDL.sql
	@gcloud spanner databases ddl update $(GOOGLE_SPANNER_DATABASE) --instance $(GOOGLE_SPANNER_INSTANCE) --ddl-file=DDL.sql

dbload: check_uv ## Load LEI data into Spanner database using uv
	@uv run load_spanner.py --instance-id $(GOOGLE_SPANNER_INSTANCE) --database-id $(GOOGLE_SPANNER_DATABASE) --project-id $(GOOGLE_PROJECT)

dbload-dryrun: check_uv ## Test data parsing, geocoding, and S2 token generation in dry-run mode
	@uv run load_spanner.py --dry-run

instancecreate: ## Spin up a single node Spanner instance
	@gcloud spanner instances create $(GOOGLE_SPANNER_INSTANCE) --description="$(GOOGLE_SPANNER_INSTANCE) Graph Database" --config=regional-$(GOOGLE_CLOUD_REGION) --edition=ENTERPRISE --default-backup-schedule-type=NONE --nodes=1

instancedelete: ## Shutdown the Spanner instance
	@gcloud spanner instances delete $(GOOGLE_SPANNER_INSTANCE)
