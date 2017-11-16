# -*- encoding: utf-8 -*-

import base64
import json
import boto3

print('Loading function')

def lambda_handler(event, context):
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = json.loads(base64.b64decode(record['kinesis']['data']))
        # payload = base64.b64decode(record['kinesis']['data'])
        print (payload)

    return 'Successfully processed {} records.'.format(len(event['Records']))