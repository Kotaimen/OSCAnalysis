# -*- encoding: utf-8 -*-

import io
import gzip
import json
import os
import xml.etree.cElementTree as etree

import boto3
try:
    import requests
except ImportError:
    import botocore.vendored.requests as requests
import botocore.exceptions

# from aws_xray_sdk.core import patch_all
# patch_all()

print('Loading function')


#
# Load globals from environment
#

FIREHOSE_STREAM_NAME = os.getenv('FIREHOSE_STREAM_NAME', None)
assert FIREHOSE_STREAM_NAME is not None

FIREHOSE_CLIENT = boto3.client('firehose')


#
# Functions
#

def split_changes(fp):
    context = etree.iterparse(fp)

    for (event, change) in context:

        if change.tag not in ('modify', 'delete', 'create'):
            continue

        yield etree.tostring(change, encoding='utf-8')

        # save memory
        change.clear()


BATCH_SIZE_LIMIT = 4 * 1024 * 1024
BATCH_SIZE_LIMIT -= 64  # be safe


def make_batch(fp, batch_limit=500, size_limit=BATCH_SIZE_LIMIT):
    records = list()
    size = 0
    for n, doc in enumerate(split_changes(fp)):

        data = doc.replace(b'\n', b' ')
        data += b'\n'

        size += len(data)

        records.append(
            dict(Data=data)
        )

        if (n % batch_limit) == 0 or (size > size_limit):
            print('Record #', n)
            yield records
            records = list()
            size = 0


#
# Entry
#
def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # dynamodb streaming events
    for record in event['Records']:
        print(record)
        if record['eventName'] != 'INSERT':
            continue

        url = record['dynamodb']['NewImage']['url']['S']

        print('Downloading from:', url)

        response = requests.get(url)
        with gzip.GzipFile(fileobj=io.BytesIO(response.content),
                           mode='r') as fp:
            for record_batch in make_batch(fp):
                FIREHOSE_CLIENT.put_record_batch(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Records=record_batch
                )

    return 'Successfully processed {} records.'.format(len(event['Records']))
