"""
Echo Server Application.
"""

import aws_manager as echo_search_aws_manager
import logging
import signal
import sys
import yaml
import hashlib
import os

logging.basicConfig(format='[%(asctime)s] [%(levelname)8s] --- %(message)s', level=logging.INFO)
aws_manager = echo_search_aws_manager.Manager('inbox', 'outbox', 'applicationtechnologies')
secret = "technology_applications_18_19"


def sigint_handler(signum, frame):
    logging.info('Stop pressing CTRL+C.')
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)

while True:
    request = aws_manager.receive_message(['echo', 'hello', 'user_id', 'user_hash', 'search'], 'inbox')

    if 'hello' in request.message_attributes:
        user_id = str(request.message_id)
        key = str(user_id + secret)
        hash_object = hashlib.md5(key.encode())
        hash_object = hash_object.hexdigest()

        logging.info('New user with id: "%s", and hash: "%s"', user_id, hash_object)

        message_attributes = {
            str(user_id): {
                'DataType': 'String',
                'StringValue': hash_object,
            }
        }
        aws_manager.send_message(message_attributes, request.body, 'outbox')
    else:
        user_id = request.message_attributes['user_id']['StringValue']
        hash_user = request.message_attributes['user_hash']['StringValue']

        key = user_id + secret
        hash_object = hashlib.md5(key.encode())
        hash_object = hash_object.hexdigest()

        if hash_object == hash_user:
            if 'echo' in request.message_attributes:
                logging.info('New echo of user "%s": with hash: "%s"', user_id, hash_user)

                message = str(request.body)

                if message.upper() == 'END':
                    logging.info('User "%s" wants to finalize the actual conversation.', user_id)
                    message = 'END'

                logging.info('Saving message in AWS S3...')

                # Download and upload appended the new conversation.
                filename = user_id + '.yaml'
                path_file = None
                path_user_yaml = aws_manager.download_file(filename=filename)

                if not path_user_yaml:
                    path_file = '/tmp/' + filename
                    logging.info('Create file for the users: "%s", path to file: "%s"', user_id, path_file)
                    with open(path_file, 'w') as yaml_file:
                        yaml.dump([message], yaml_file, default_flow_style=False)
                else:
                    path_file = path_user_yaml

                    with open(path_file, 'r') as stream:
                        try:
                            user_yaml = yaml.load(stream)
                            user_yaml.append(message)

                            with open(path_file, 'w') as yaml_file:
                                yaml.dump(user_yaml, yaml_file, default_flow_style=False)
                        except yaml.YAMLError as exc:
                            logging.info(exc)

                upload_ok = aws_manager.upload_file(path_file=path_file, filename=filename)

                if upload_ok:
                    os.remove(path=path_file)
                    logging.info('Local file with path "%s" removed.', path_file)

                # ECHO reply
                message_attributes = {
                    str(user_id): {
                        'DataType': 'String',
                        'StringValue': 'identifier'
                    }
                }
                aws_manager.send_message(message_attributes, 'ECHO   MESSAGE: ' + request.body, 'outbox')
            else:
                if 'search' in request.message_attributes:
                    logging.info('User "%s": with hash: "%s" wants to get its conversations.', user_id, hash_user)

                    filename = user_id + '.yaml'
                    path_file = aws_manager.get_url(filename=filename)

                    if not path_file:
                        message = 'User hasn\'t any previous conversations'
                        logging.info(message)

                        message_attributes = {
                            str(user_id): {
                                'DataType': 'String',
                                'StringValue': 'False'
                            }
                        }

                        aws_manager.send_message(message_attributes, str(message), 'outbox')
                    else:
                        message = path_file
                        logging.info('Path file: %s', message)

                        message_attributes = {
                            str(user_id): {
                                'DataType': 'String',
                                'StringValue': 'identifier'
                            }
                        }

                        aws_manager.send_message(message_attributes, str(message), 'outbox')
                else:
                    logging.info('Attribute not valid.')
        else:
            logging.info('New message of user not registered "%s", server is not going to send any message.', user_id)
