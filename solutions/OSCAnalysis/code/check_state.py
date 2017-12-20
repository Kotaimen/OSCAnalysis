# -*- encoding: utf-8 -*-

import time
import os
import configparser

import boto3
# Use requests bundled with boto3 so we don't need package lambda function
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

OSC_STATE_ROOT = 'http://planet.openstreetmap.org/replication/'

OSC_INTERVAL = os.getenv('OSC_INTERVAL', 'minute')
OSC_TABLE_NAME = os.getenv('OSC_TABLE_NAME', None)
assert OSC_TABLE_NAME is not None

dynamodb = boto3.resource('dynamodb')
OSC_TABLE = dynamodb.Table(OSC_TABLE_NAME)

# by default, expire items in an hour
TABLE_ITEM_TTL = 3600


#
# Functions
#

def get_latest_sequence(root_url, interval='minute'):
    """#Fri Nov 10 03:02:09 UTC 2017
    sequenceNumber=45236
    timestamp=2017-11-10T03\:00\:00Z
    """
    parser = configparser.ConfigParser()
    response = requests.get('%s%s/state.txt' % (root_url, interval))

    # HACK: pretend its a config file
    config_file = '[state]\n%s' % response.text
    parser.read_string(config_file)

    sequence = parser.getint('state', 'sequenceNumber')
    # reformat to timestamp
    timestamp = parser.get('state', 'timestamp').replace(r'\:', r':')

    print('Sequence #%d at %s' % (sequence, timestamp))

    return sequence, timestamp


def write_one_state(root_url, interval, table, sequence, timestamp, ttl):
    # path format is 000/000/000
    path = '%09d' % sequence
    path = '/'.join([path[:3], path[3:6], path[6:]])

    print('Writing sequence #%d' % sequence)

    table.put_item(
        Item={
            'seq': sequence,
            'url': '%s%s/%s.osc.gz' % (root_url, interval, path),
            'ttl': int(time.time()) + ttl
        },
        # prevent overwrite existing item
        ConditionExpression="attribute_not_exists(seq)"
    )

#
# Entry
#
def lambda_handler(event, context):
    # get latest sequence
    sequence, timestamp = \
        get_latest_sequence(OSC_STATE_ROOT,
                            interval=OSC_INTERVAL)

    # Write last three states to DDB
    # XXX: should scan DDB table for missing states
    for seq in range(sequence, sequence - 3, -1):
        try:
            write_one_state(
                OSC_STATE_ROOT, OSC_INTERVAL,
                OSC_TABLE, seq, timestamp,
                TABLE_ITEM_TTL)
        except botocore.exceptions.ClientError as e:
            print('Skipped sequence %d: %r' % (seq, e))
            break
