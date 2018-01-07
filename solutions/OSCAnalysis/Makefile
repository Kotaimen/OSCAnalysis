
AWS_PROFILE=demo
AWS_REGION=us-west-2
ARTIFACT_BUCKET_NAME=awscfncli-716160732812-us-west-2

# aws/s3 key arn
KMS_KEY_ARN=arn:aws:kms:us-west-2:716160732812:key/8f55e6ac-35f6-4a06-bf5b-767620a812f4

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
			DatalakeStack=${DATALAKE_STACK_NAME};

etl: ETL.template.yaml
	aws cloudformation deploy \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--capabilities CAPABILITY_IAM \
		--template-file ETL.template.yaml \
		--stack-name OscAnalysis3-ETL \
		--parameter-overrides \
			KmsKeyArn=${KMS_KEY_ARN} \
			CuratedDatabaseName=oscanalysis3-curated-database \
			StagingDatabaseName=oscanalysis3-staging-database \
			OscCuratedPrefix=openstreetmap/replication/changes \
			DatalakeStack=${DATALAKE_STACK_NAME};



#deploy: datalake ingest
