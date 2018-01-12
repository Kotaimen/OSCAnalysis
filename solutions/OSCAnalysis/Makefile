# global paremeters
AWS_PROFILE=default
AWS_REGION=us-east-1
ARTIFACT_BUCKET_NAME=awscfncli-889515947644-us-east-1

# aws/s3 key arn
KMS_KEY_ARN=arn:aws:kms:us-east-1:889515947644:key/89f70286-6beb-43be-a847-e91563fc81a4

DATALAKE_STACK_NAME=OscAnalysis3-DataLake

datalake: Buckets.template.yaml
	aws cloudformation deploy \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--template-file Buckets.template.yaml \
		--stack-name ${DATALAKE_STACK_NAME} \
		--parameter-overrides KmsKeyArn=${KMS_KEY_ARN}

ingest: Ingest.template.yaml
	output_template_file=$$(mktemp); \
	aws cloudformation package \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--template-file $^ \
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


dashboard: Dashboard.template.yaml
	output_template_file=$$(mktemp); \
	aws cloudformation package \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--template-file $^ \
		--s3-bucket ${ARTIFACT_BUCKET_NAME} \
		--output-template-file $${output_template_file}; \
	aws cloudformation deploy \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--capabilities CAPABILITY_IAM \
		--template-file $${output_template_file} \
		--stack-name OscAnalysis3-Dashboard \
		--parameter-overrides \
			IngestStack=OscAnalysis3-Ingest;


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
			OscStagingPrefix=osc/changes \
			DatalakeStack=${DATALAKE_STACK_NAME};

redshift: Redshift.template.yaml
	aws cloudformation deploy \
		--profile ${AWS_PROFILE} \
		--region ${AWS_REGION} \
		--capabilities CAPABILITY_IAM \
		--template-file $^ \
		--stack-name OscAnalysis3-Redshift \
		--parameter-overrides \
			DatabaseName=osc \
			PubliclyAccessible=true \
			RedshiftInboundCIDR=180.168.157.96/28 \
			RedshiftUsername=awsuser \
			RedshiftPassword=Cheeseshop123 \
			VpcId=vpc-df31bbba \
			SubnetId1=subnet-76388d01 \
			SubnetId2=subnet-8efa0ca5 \
			DatalakeStack=${DATALAKE_STACK_NAME} \
			;

#deploy: datalake ingest
