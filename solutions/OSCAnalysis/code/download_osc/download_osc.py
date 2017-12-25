# -*- encoding: utf-8 -*-


import io
import gzip
import json
import os
import xml.etree.cElementTree as etree

import boto3

print('Loading function')

try:
    import requests
except ImportError:
    import botocore.vendored.requests as requests


# Try to patch all supported libraries for x-ray
try:
    from aws_xray_sdk.core import patch_all
    patch_all()
except ImportError:
    print('Skipping aws_xray_sdk')


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
    """ Split OSC change into json documents

    OSC XML Schema:
        <change>
            <feature attrib=...>
                <tags attrib=... >
    Output doc schema:
        - change: one of modify|delete|create
        - feature: one of node|way|relation
        - id: big int
        - version: big int
        - timestamp: "2012-09-12T23:49:25Z"
        - uid: int
        - user: string
        - changeset: int
        - tags: map of tags
        - lon: float (only on nodes)
        - lat: float (only on nodes)
        - node_ref: array of referenced nodes (only on ways)
        - node_member: map of member->role (only on relations)
        - way_member: map of member->role (only on relations)
    """
    context = etree.iterparse(fp)
    for (event, change) in context:
        if change.tag not in ('modify', 'delete', 'create'):
            continue

        for feature in change:
            doc = dict(
                change=change.tag,
                feature=feature.tag,
                id=int(feature.attrib['id']),
                version=int(feature.attrib['version']),
                timestamp=feature.attrib['timestamp'],
                uid=int(feature.attrib['uid']),
                user=feature.attrib['user'],
                changeset=int(feature.attrib['changeset']),
            )

            doc['tags'] = dict((e.attrib['k'], e.attrib['v']) for e in
                               feature.findall('tag'))

            if feature.tag == 'node':
                doc['lon'] = float(feature.attrib['lon'])
                doc['lat'] = float(feature.attrib['lat'])
            elif feature.tag == 'way':
                doc['node_ref'] = list(int(e.attrib['ref']) for e in
                                       feature.findall('nd'))
            elif feature.tag == 'relation':
                doc['node_member'] = dict(
                    (int(e.attrib['ref']), e.attrib['role']) for e in
                    feature.findall("member[@type='node']"))
                doc['way_member'] = dict(
                    (int(e.attrib['ref']), e.attrib['role']) for e in
                    feature.findall("member[@type='way']"))
            yield doc
        else:
            change.clear()


def serialize_doc(doc):
    """Serialize JSON doc as utf-8"""
    # one liner json
    data = json.dumps(doc, ensure_ascii=False, indent=None)
    payload = b'%b\n' % data.encode('utf-8')
    return payload


def make_batch(fp, batch_limit=500,
               batch_size_limit=4194304,
               record_size_limit=1024000):
    """ Make Firehose record batch, which must be either:
     - less than 500 records
     - less than 4MB in total size
     - each record must be smaller than 1000KB

    Batch schema:
        [
            {
                "Data": <payload>
            },
            ...
        ]
    """
    records = list()
    size = 0
    for n, doc in enumerate(split_changes(fp)):

        payload = serialize_doc(doc)
        payload_size = len(payload)

        if payload_size > record_size_limit:
            print('Drooping oversize record #%d (%d bytes)' % (n, payload_size))
            continue

        if (n > 0 and (n % batch_limit) == 0) or \
                (size + len(payload)) > batch_size_limit:
            print('Record #%d (%s bytes)' % (n, size))
            yield records
            records = list()
            size = 0

        records.append(
            dict(Data=payload)
        )
        size += payload_size
    else:
        print('Record #%d (%s)' % (n, size))
        yield records


#
# Entry
#
def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # DDB streaming events
    for record in event['Records']:
        # print(record)

        if record['eventName'] != 'INSERT':  # only deal with insert events
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
