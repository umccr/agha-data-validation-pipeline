#!/usr/bin/env python3
import json
import logging


import boto3


import util


# Setup logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Get AWS clients
CLIENT_BATCH = util.get_client('batch')


def get_event_data(key, event):
    if not (value := event.get(key)):
        LOGGER.critical(f'event data did not contain \'{key}\' value')
        sys.exit(1)
    else:
        LOGGER.info(f'found \'{key}\' event data: {value}')
    return value


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(util.get_context_info(context))}')

    # Collect run parameters from event
    command = get_event_data('command', event)
    environment = get_event_data('environment', event)
    job_name = get_event_data('job_name', event)
    job_queue = get_event_data('job_queue', event)
    job_definition = get_event_data('job_definition', event)

    # Submit job
    response = CLIENT_BATCH.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            'memory': 4000,
            'environment': environment,
            'command': command
        }
    )
    LOGGER.info(f'submission response: {response}')
