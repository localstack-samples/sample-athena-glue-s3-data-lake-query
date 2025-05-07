export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION=us-east-1
SHELL := /bin/bash

usage:			## Show this help in table format
	@echo "| Target                 | Description                                                       |"
	@echo "|------------------------|-------------------------------------------------------------------|"
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "| %-22s | %-65s |\n", $$1, $$2 }'

check:			## Check if all required prerequisites are installed
	@command -v docker > /dev/null 2>&1 || { echo "Docker is not installed. Please install Docker and try again."; exit 1; }
	@command -v aws > /dev/null 2>&1 || { echo "AWS CLI is not installed. Please install AWS CLI and try again."; exit 1; }
	@command -v awslocal > /dev/null 2>&1 || { echo "AWS CLI Local is not installed. Please install awslocal and try again."; exit 1; }
	@command -v localstack > /dev/null 2>&1 || { echo "LocalStack is not installed. Please install LocalStack and try again."; exit 1; }
	@command -v python > /dev/null 2>&1 || { echo "Python is not installed. Please install Python and try again."; exit 1; }
	@echo "All required prerequisites are available."

deploy:		## Setup the architecture
	@echo "Deploying the architecture..."
	@echo "Create S3 bucket and upload the CloudFormation template..."
	awslocal s3 mb s3://covid19-lake; \
	awslocal s3 cp cloudformation-templates/CovidLakeStack.template.json s3://covid19-lake/cfn/CovidLakeStack.template.json; \
	awslocal s3 sync ./covid19-lake-data/ s3://covid19-lake/; \
	awslocal cloudformation create-stack --stack-name covid-lake-stack --template-url http://s3.localhost.localstack.cloud:4566/covid19-lake/cfn/CovidLakeStack.template.json
	@counter=0; \
	while [ $$counter -lt 30 ]; do \
		status=$$(awslocal cloudformation describe-stacks --stack-name covid-lake-stack | grep StackStatus | cut -d'"' -f4); \
		echo "Attempt $$counter: Stack status: $$status"; \
		if [ "$$status" = "CREATE_COMPLETE" ]; then \
			echo "Stack creation completed successfully!"; \
			exit 0; \
		fi; \
		counter=$$((counter+1)); \
		if [ $$counter -lt 30 ]; then sleep 2; fi; \
	done; \
	echo "Stack creation timed out after 30 attempts"; \
	exit 1

test:		## Run the tests
	@echo "Installing dependencies..."
	@pip install -r tests/requirements.txt
	@echo "Running tests..."
	pytest -s -v --disable-warnings tests/
	@echo "All tests completed successfully."

start:		## Start LocalStack
	@echo "Starting LocalStack..."
	@LOCALSTACK_AUTH_TOKEN=$(LOCALSTACK_AUTH_TOKEN) IMAGE_NAME=localstack/localstack-pro:latest-bigdata localstack start -d
	@echo "LocalStack started successfully."

stop:		## Stop the Running LocalStack container
	@echo "Stopping LocalStack..."
	localstack stop

ready:		## Make sure the LocalStack container is up
	@echo Waiting on the LocalStack container...
	@localstack wait -t 30 && echo LocalStack is ready to use! || (echo Gave up waiting on LocalStack, exiting. && exit 1)

logs:     ## Save the LocalStack logs in a separate file
	@localstack logs > logs.txt

.PHONY: usage install run start stop ready logs
