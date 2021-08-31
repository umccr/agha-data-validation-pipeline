#!/usr/bin/env python3
import logging
import json
import sys


import util


import event_manual
import event_s3


# Logging and message store
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(util.get_context_info(context))}')

    # Handle S3 event records
    if 'Records' in event:
        event_set = [er for er in event['Records']]
    else:
        event_set = [event]
    # Process events
    for event in event_set:
        LOGGER.info(f'processing: {event}')
        if event.get('eventSource') == 'aws:s3':
            event_s3.handler(event)
        else:
            event_manual.handler(event)
