
AWS_PROFILE=demo
AWS_REGION=us-west-2
ARTIFACT_BUCKET_NAME=awscfncli-716160732812-us-west-2


KMS_KEY_ARN=arn:aws:kms:us-west-2:716160732812:key/115e0300-6fe4-4470-a0eb-845545693156

DATALAKE_STACK_NAME=OscAnalysis3-DataLake

datalake: Buckets.template.yaml
	aws cloudformation deploy \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--template-file Buckets.template.yaml \
		--stack-name ${DATALAKE_STACK_NAME} \
		--parameter-overrides KmsKeyArn=${KMS_KEY_ARN}

ingest:
	output_template_file=$$(mktemp); \
	aws cloudformation package \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--template-file Ingest.template.yaml \
		--s3-bucket ${ARTIFACT_BUCKET_NAME} \
		--output-template-file $${output_template_file}; \
	aws cloudformation deploy \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--capabilities CAPABILITY_IAM \
		--template-file $${output_template_file} \
		--stack-name OscAnalysis3-Ingest \
		--parameter-overrides \
			KmsKeyArn=${KMS_KEY_ARN} \
			DatalakeStack=${DATALAKE_STACK_NAME}; \

