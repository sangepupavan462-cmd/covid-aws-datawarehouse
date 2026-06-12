"""
AWS Lambda — COVID-19 Ingestion Trigger
-----------------------------------------
Triggered by S3 PUT events on the Bronze bucket.
Logs file metadata to CloudWatch for ingestion audit trail.

Deploy:
  - Runtime   : Python 3.11
  - Trigger   : S3 ObjectCreated:Put on covid-de-bronze-* bucket
  - Timeout   : 30 seconds
  - IAM       : Lambda basic execution role + CloudWatchLogsFullAccess
"""

import json
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Fires on every new file uploaded to the Bronze S3 bucket.
    Logs ingestion metadata to CloudWatch Logs.
    """
    records = []

    for record in event.get('Records', []):
        bucket     = record['s3']['bucket']['name']
        key        = record['s3']['object']['key']
        size_bytes = record['s3']['object'].get('size', 0)
        event_time = record['eventTime']

        size_mb  = round(size_bytes / (1024 * 1024), 2)
        file_ext = key.split('.')[-1].upper() if '.' in key else 'UNKNOWN'

        # Determine layer from bucket name
        if 'bronze' in bucket:
            layer = 'BRONZE'
        elif 'silver' in bucket:
            layer = 'SILVER'
        elif 'gold' in bucket:
            layer = 'GOLD'
        else:
            layer = 'UNKNOWN'

        log_entry = {
            'timestamp'  : datetime.now(timezone.utc).isoformat(),
            'event_time' : event_time,
            'project'    : 'COVID-19 Analytics',
            'layer'      : layer,
            'bucket'     : bucket,
            'file_key'   : key,
            'file_ext'   : file_ext,
            'size_mb'    : size_mb,
            'status'     : 'INGESTED'
        }

        logger.info(f'[INGESTION] {json.dumps(log_entry)}')
        records.append(log_entry)

    logger.info(f'[SUMMARY] {len(records)} file(s) processed.')

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message' : f'{len(records)} file(s) ingested successfully',
            'records' : records
        })
    }
