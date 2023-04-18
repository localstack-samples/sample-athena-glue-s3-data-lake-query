export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION=us-east-1
SHELL := /bin/bash

usage:    ## Show this help
		@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

install:  ## Install dependencies
		@which localstack || pip install localstack
		@which awslocal || pip install awscli-local
		@which tflocal || pip install terraform-local

setup:    ## Setup the architecture
		awslocal s3 mb s3://covid19-lake; \
		awslocal s3 cp CovidLakeStack.template.json s3://covid19-lake/cfn/CovidLakeStack.template.json; \
		awslocal s3 sync ./covid19-lake-data/ s3://covid19-lake/; \
		awslocal cloudformation create-stack --stack-name covid-lake-stack --template-url https://covid19-lake.s3.us-east-2.amazonaws.com/cfn/CovidLakeStack.template.json

start:    ## Start LocalStack in detached mode
		localstack start -d

stop:     ## Stop the Running LocalStack container
		@echo
		localstack stop

ready:    ## Make sure the LocalStack container is up
		@echo Waiting on the LocalStack container...
		@localstack wait -t 30 && echo LocalStack is ready to use! || (echo Gave up waiting on LocalStack, exiting. && exit 1)

logs:     ## Save the LocalStack logs in a separate file
		@localstack logs > logs.txt

.PHONY: usage install run start stop ready logs
