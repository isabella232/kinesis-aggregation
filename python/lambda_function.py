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

def lambda_handler(event, context):
    # Get the S3 object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        compressed = BytesIO(response['Body'].read())
        decompressed = GzipFile(None, 'rb', fileobj=compressed).read()
        lines = decompressed.split('\n')
        records = []
        res = None
        failures = 0
        kinesis_aggregator = aws_kinesis_agg.aggregator.RecordAggregator()
        for idx, line in enumerate(lines):
            # just use random partition key (could use user_id for transform)
            partition_key = str(uuid.uuid4())
            explicit_hash_key = str(uuid.uuid4().int)
            rec = {'Data': line, 'PartitionKey': partition_key, 'ExplicitHashKey': explicit_hash_key}
            # keep adding records to the aggregator until it's full
            result = kinesis_aggregator.add_user_record(rec['PartitionKey'], rec['Data'], rec['ExplicitHashKey'])
            if result:
                records.append(rec)
                kinesis_aggregator = aws_kinesis_agg.aggregator.RecordAggregator()
                if len(records) > 499:
                    res = kinesis.put_records(Records=records, StreamName="staging-transform")
                    failures += res['FailedRecordCount']
                    time.sleep(1)
                    records = []
        if len(records) > 0:
            res = kinesis.put_records(Records=records, StreamName="staging-transform")
            failures += res['FailedRecordCount']
        return failures
    except Exception as e:
        print(e)
        raise e
