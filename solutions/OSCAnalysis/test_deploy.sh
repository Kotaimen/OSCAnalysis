#!/usr/bin/env bash

#
# Deploy the SAM template (for testing purpose)
#
set -ex

export AWS_DEFAULT_REGION=us-east-1
#export AWS_PROFILE=
export PACKAGE_S3_BUCKET=flyingbear-us-east-1
export PACKAGE_S3_PREFIX=resources/sam/OSCAnalysis
export STACK_NAME=OscAnalysis

PACKAGED_TEMPLATE=$(mktemp)
echo Packaged template: $mktemp

aws cloudformation package \
    --template-file ./OscAnalysis.template.yaml \
    --s3-bucket $PACKAGE_S3_BUCKET \
    --s3-prefix $PACKAGE_S3_PREFIX \
    --output-template-file $PACKAGED_TEMPLATE

aws cloudformation deploy \
    --template-file $PACKAGED_TEMPLATE \
    --stack-name $STACK_NAME \
    --capabilities CAPABILITY_IAM
