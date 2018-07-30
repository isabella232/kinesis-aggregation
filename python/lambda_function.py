# Kinesis Aggregation/Deaggregation Libraries for Python
# 
# Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved. 
# 
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
# 
#  http://aws.amazon.com/asl/
# 
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import print_function

import aws_kinesis_agg.aggregator
import uuid

import json
import urllib
import boto3
from io import BytesIO
from gzip import GzipFile
import time

s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')
records = []
failures = 0

def collect_record(record):
    global records
    pk, ehk, data = record.get_contents()
    res = kinesis.put_record(StreamName='staging-transform',
                       Data=data,
                       PartitionKey=pk,
                       ExplicitHashKey=ehk)
    # Stupid hack to reduce throttling
    time.sleep(1)

def lambda_handler(event, context):
    global failures
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        # Get the S3 object from the event
        response = s3.get_object(Bucket=bucket, Key=key)
        compressed = BytesIO(response['Body'].read())
        decompressed = GzipFile(None, 'rb', fileobj=compressed).read()
        lines = decompressed.split('\n')

        # Initialize kinesis aggregator
        kinesis_aggregator = aws_kinesis_agg.aggregator.RecordAggregator()
        kinesis_aggregator.on_record_complete(collect_record)
        for idx, line in enumerate(lines):
            # just use random partition key (could use user_id for transform)
            partition_key = str(uuid.uuid4())
            explicit_hash_key = str(uuid.uuid4().int)
            # keep adding records to the aggregator until it's full
            kinesis_aggregator.add_user_record(partition_key, line, explicit_hash_key)
        if len(records) > 0:
            collect_record(kinesis_aggregator.clear_and_get())
            res = kinesis.put_records(Records=records, StreamName="staging-transform")
            failures += res['FailedRecordCount']
        return failures
    except Exception as e:
        print(e)
        # TODO: handle backoff
        pass
