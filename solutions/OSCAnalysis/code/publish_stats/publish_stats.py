# -*- encoding: utf-8 -*-

import base64
import json
import boto3
import datetime

print('Loading function')

# Try to patch all supported libraries for x-ray
try:
    from aws_xray_sdk.core import patch_all
    patch_all()
except ImportError:
    print('Skipping aws_xray_sdk')

cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, context):

    metric_data = []

    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = json.loads(base64.b64decode(record['kinesis']['data']))
        # payload = base64.b64decode(record['kinesis']['data'])
        print(payload)

        # Use client generated time (changset time in OSM)
        timestamp = datetime.datetime.strptime(
            payload['client_time'].split('.')[0],
            '%Y-%m-%d %H:%M:%S')

        data = dict(
            MetricName=payload['category'],
            Dimensions=[
                dict(
                    Name='Group Name',
                    Value=payload['group_name']
                ),
            ],
            Timestamp=timestamp,
            Value=payload['item_count'],
            Unit='Count',
            StorageResolution=60
        )

        metric_data.append(data)

    result = cloudwatch.put_metric_data(
        Namespace='OSM/OSC',
        MetricData=metric_data
    )

    print(result)

    return 'Successfully processed {} records.'.format(len(event['Records']))
