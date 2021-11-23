#!/usr/bin/env python3
import argparse
import enum
import json
import logging

import util

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Tasks(enum.Enum):

    COPY = 'copy'
    DELETE = 'delete'
    COPY_AND_DELETE = 'copy_and_delete'

def main():
    logger.info('Execute S3 batch manipulation')

    # Get command line arguments
    args = get_arguments()

    logger.info('Processing args value received:')
    logger.info(json.dumps(args))

    # s3 client
    s3_client = util.get_client('s3')

    task = args.task
    source_bucket_name = args.source_bucket_name
    target_bucket_name = args.target_bucket_name
    source_directory = args.source_directory
    target_directory = args.target_directory
    

    if task in [Tasks.COPY.value, Tasks.COPY_AND_DELETE.value]:
        logger.info("Run COPY object task")

        s3_client_copy = s3_client.meta.client.copy

        copy_source = {
            'Bucket': source_bucket_name,
            'Key': source_directory
        }
        try:
            copy_response = s3_client_copy(copy_source, target_bucket_name, target_directory)
        except:
            logger.warn("Copy file unsuccessful")
            has_error = True
        finally:
            logger.info("s3 Copy object response:")
            logger.info(json.dump(copy_response))
        
        if has_error:
            logger.warn('Terminating. Something went wrong on copy object')
            raise ValueError('Something wrong on S3_object copy')

        
    if task in [Tasks.DELETE.value, Tasks.COPY_AND_DELETE.value]:
        logger.info("Run DELETE object task")
        
        s3_client_delete = s3_client.delete_object

        try:
            s3_client_delete(source_bucket_name, source_directory)
        except:
            logger.warn("Copy file unsuccessful")
            has_error = True
        finally:
            logger.info("s3 Copy object response:")
            logger.info(json.dump(copy_response))
        
        if has_error:
            logger.warn('Terminating. Something went wrong on deleting object')
            raise ValueError('Something wrong on S3_object delete')

    logger.info('Successfully run s3 manipulation batch job')

def get_arguments():
    parser = argparse.ArgumentParser()
    # Source
    parser.add_argument('--source_bucket_name', required=True, type=str,
        help='S3 source bucket name')
    parser.add_argument('--source_directory', required=True, type=str,
        help='S3 source object directory to be copied')
    # Target
    parser.add_argument('--target_directory', required=False, type=str,
        help='S3 target object directory to be copied')
    parser.add_argument('--target_bucket_name', required=False, type=str,
        help='S3 target bucket name')
    # Type of tasks
    parser.add_argument('--task', required=True, choices=[m.value for m in Tasks],
        help='Task to perform. Choices: copy, delete, copy_and_delete')
    return parser.parse_args()


if __name__ == '__main__':
    main()