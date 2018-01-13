#!/usr/bin/env bash

# Start a small debugging cluster with glue catalog

aws emr create-cluster \
    --region us-east-1 \
    --name 'Fat Ray' \
    --release-label emr-5.11.0 \
    --applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Ganglia Name=Presto Name=Spark Name=Livy \
    --log-uri 's3n://aws-logs-889515947644-us-east-1/elasticmapreduce/' \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --ebs-root-volume-size 32 \
    --service-role EMR_DefaultRole \
    --enable-debugging \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --termination-protected \
    --ec2-attributes file://ec2-attributes.json \
    --instance-groups file://instance-groups.json \
    --configurations file://configurations.json \
