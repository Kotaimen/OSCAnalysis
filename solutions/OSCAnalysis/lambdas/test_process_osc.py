# -*- encoding: utf-8 -*-

import os
import sys
import json

import boto3
import moto

# insert lambda package path
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'process_osc'))

from process_osc import lambda_handler

BUCKET_NAME = 'boba'
OSC_FILENAME=os.path.join('test_data/', 'hour001.osc.gz')

OSC_PREFIX = 'replication/hour/000/000/001.osc.gz'

# Sample S3 notification event in SNS event
SAMPLE_EVENT = \
    {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": "foobar",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": "foobar",
                    "TopicArn": "arn:aws:sns:us-west-2:1234567890:foobar",
                    "Subject": "Amazon S3 Notification",
                    "Message": json.dumps(
                        {
                            "Records": [
                                {
                                    "eventVersion": "2.0",
                                    "eventSource": "aws:s3",
                                    "awsRegion": "us-east-1",
                                    "eventTime": "1970-01-01T00:00:00.000Z",
                                    "eventName": "ObjectCreated:Put",
                                    "userIdentity": {
                                        "principalId": "foobar"
                                    },
                                    "requestParameters": {
                                        "sourceIPAddress": "127.0.0.1"
                                    },
                                    "responseElements": {
                                        "x-amz-request-id": "foobar",
                                        "x-amz-id-2": "foobar"
                                    },
                                    "s3": {
                                        "s3SchemaVersion": "1.0",
                                        "configurationId": "testConfigRule",
                                        "bucket": {
                                            "name": BUCKET_NAME,
                                            "ownerIdentity": {
                                                "principalId": "foobar"
                                            },
                                            "arn": "arn:aws:s3:::%s" % BUCKET_NAME
                                        },
                                        "object": {
                                            "key": OSC_PREFIX,
                                            "size": os.path.getsize(OSC_FILENAME),
                                            "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                                            "versionId": "096fKKXTRTtl3on89fVO.nfljtsv6qko",
                                            "sequencer": "0055AED6DCD90281E5"
                                        }
                                    }
                                }
                            ]
                        }
                    ),
                    "Timestamp": "2018-01-03T07:32:27.866Z",
                    "SignatureVersion": "1",
                    "Signature": "foobar",
                    "SigningCertUrl": "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-433026a4050d206028891664da859041.pem",
                    "UnsubscribeUrl": "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:716160732812:foobar:foobar",
                    "MessageAttributes": {}
                }
            }
        ]
    }


@moto.mock_s3
def test_lambda_handler():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=BUCKET_NAME)
    s3.upload_file(OSC_FILENAME,BUCKET_NAME,OSC_PREFIX,)
    lambda_handler(SAMPLE_EVENT, {})

