"""
AWS Manager for EchoSearch Application.

Class for manage all communications with AWS.

Author: Ruben Perez Vaz for Lab Assignment 1.
"""

import boto3
from botocore.client import ClientError
import time
import logging
import os


class Manager:
    def __init__(self, name_inbox_queue, name_outbox_queue, name_bucket):
        logging.info('Initializing manager...')
        self.sqs = boto3.resource('sqs')
        self.s3 = boto3.client('s3')
        self.name_bucket = name_bucket
        self.queue_inbox = self.sqs.get_queue_by_name(QueueName=name_inbox_queue)
        self.queue_outbox = self.sqs.get_queue_by_name(QueueName=name_outbox_queue)
        logging.info('Manager initialized.')

        return

    '''
    Used for send messages to a specific queue.
    :return
        > SQS.Message() [see boto3 SQS.Message]
    '''

    def send_message(self, message_attributes, message, queue_name):
        logging.info(
            'Entering send_message(), arguments [message_attributes = "%s", message = "%s", queue_name = "%s"]',
            str(message_attributes),
            str(message),
            str(queue_name))

        if queue_name is 'inbox':
            logging.info('  Send new message to queue "inbox".')
            queue = self.queue_inbox
        else:
            if queue_name is 'outbox':
                logging.info('  Send new message to queue "outbox".')

                queue = self.queue_outbox
            else:
                logging.ERROR(' Queue not valid, exit!')

                return -1

        response = queue.send_message(MessageAttributes=message_attributes, MessageBody=str(message))

        logging.info('  Message send.')
        logging.info('      > MD5OfMessageBody: "%s".', str(response['MD5OfMessageBody']))
        logging.info('      > MD5OfMessageAttributes: "%s".', str(response['MD5OfMessageAttributes']))
        logging.info('      > MessageId: "%s".', str(response['MessageId']))

        logging.info("Leaving send_message()")

        return response

    '''
    Used for receive a message from a queue using a list of filters in order to gets only the correct messages.
    :return
        > SQS.Message() [see boto3 SQS.Message]
    '''

    def receive_message(self, list_identifiers, queue_name):
        logging.info('Entering receive_message(), arguments [list_identifiers = "%s", queue_name = "%s"]',
                     str(list_identifiers), str(queue_name))

        if queue_name is 'inbox':
            logging.info('  Waiting for new message from queue "inbox".')
            queue = self.queue_inbox
        else:
            if queue_name is 'outbox':
                logging.info('  Waiting for new message from queue "outbox".')
                queue = self.queue_outbox
            else:
                logging.ERROR(' Queue not valid, exit!')
                return -1

        while True:
            for response in queue.receive_messages(MessageAttributeNames=list_identifiers, MaxNumberOfMessages=1,
                                                   VisibilityTimeout=100, WaitTimeSeconds=20):
                if response.message_attributes is not None:
                    logging.info('      Receive new message:')
                    logging.info('          > body: "%s".', str(response.body))
                    logging.info('          > attributes: "%s".', str(response.attributes))
                    logging.info('          > md5_of_body: "%s".', str(response.md5_of_body))
                    logging.info('          > md5_of_message_attributes: "%s".',
                                 str(response.md5_of_message_attributes))
                    logging.info('          > message_attributes: "%s".', str(response.message_attributes))
                    logging.info('          > message_id: "%s".', str(response.message_id))
                    response.delete()

                    logging.info("Leaving receive_message()")
                    return response
                else:
                    response.change_visibility(VisibilityTimeout=0)
                    time.sleep(5)

    '''
    Get the url of a specific file in a bucket.
    :return
        > False: If not exist the file in the bucket.
        > Path: path to filename.    
    '''

    def get_url(self, filename):
        try:
            self.s3.head_object(Bucket=self.name_bucket, Key=filename)
        except ClientError:
            return False

        return self.s3.generate_presigned_url('get_object', Params={'Bucket': self.name_bucket, 'Key': filename})

    '''
    Download a specific file in a bucket and stored in /tmp directory.
    :return
        > False: if not exist the file in the bucket.
        > dir_store: path to the file in /tmp directory
            
    '''

    def download_file(self, filename):
        try:
            dir_store = '/tmp/' + filename

            logging.info('Try to download "%s" from bucket "%s" and stored in "%s"...',
                         str(filename), str(self.name_bucket), str(dir_store))

            self.s3.download_file(self.name_bucket, filename, dir_store)
        except ClientError:
            logging.info('File "%s" does not exist.', str(filename))

            return False

        logging.info('Download successfully the file "%s" and stored in "%s".', str(filename), str(dir_store))

        return dir_store

    '''
    Upload a file to a bucket.
    :return
        > True: If could upload the file.
        > False: If couldn't upload the file.
    '''

    def upload_file(self, path_file, filename):
        try:
            self.s3.upload_file(path_file, self.name_bucket, filename)
        except FileNotFoundError as err:
            logging.info(err)

            return False

        logging.info('Upload file "%s" to bucket "%s"', str(path_file), str(self.name_bucket))

        return True
