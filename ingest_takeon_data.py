import json
import logging
import os
import ast
# import random

import boto3
import marshmallow
# import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError

# Clients
s3 = boto3.resource('s3', region_name='eu-west-2')
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')


class InputSchema(marshmallow.Schema):
    """
    Scheme to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = marshmallow.fields.str(required=True)
    file_name = marshmallow.fields.str(required=True)
    period = marshmallow.fields.str(required=True)
    sqs_queue_url = marshmallow.fields.str(required=True)
    sqs_messageid_name = marshmallow.fields.str(required=True)
    sns_topic_arn = marshmallow.fields.str(required=True)
    question_codes = marshmallow.fields.str(required=True)
    question_labels = marshmallow.fields.str(required=True)


def lambda_handler(event, context):
    """
    This method will ingest data from Take On S3 bucket, transform it so that it fits
    in the results pipeline, and send it to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: Success - True/False & Checkpoint
    """
    current_module = "BMI Results Data Ingest"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Results Data Ingest")
    logger.setLevel(10)
    try:
        logger.info("Running Results Data Ingest...")

        # Needs to be declared inside the lambda_handler
        # lambda_client = boto3.client('lambda', region_name='eu-west-2')

        # ENV vars
        config, errors = InputSchema().load(os.environ)
        bucket_name = config['bucket_name']
        file_name = config['file_name']
        period = config['period']
        # sqs_queue_url = config['sqs_queue_url']
        # sqs_messageid_name = config['sqs_messageid_name']
        # sns_topic_arn = config['sns_topic_arn']
        question_codes = ast.literal_eval(config['question_codes'])
        question_labels = ast.literal_eval(config['question_labels'])
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated environment parameters.")

        input_file = read_from_s3(bucket_name, file_name)

        # gigantic extraction loop goes here
        # ...
        input_json = json.load(input_file)
        output_json = []
        for survey in input_json['data']['allSurveys']['nodes']:
            if survey['survey'] == "066" or survey['survey'] == "076":
                for contributor in survey['contributorsBySurvey']['nodes']:
                    if contributor['period'] == period:
                        outContrib = {}
                        outContrib['period'] = contributor['period']
                        outContrib['responder_id'] = contributor['reference']
                        outContrib['gor_code'] = contributor['region']
                        outContrib['enterprise_ref'] = contributor['enterprisereference']
                        outContrib['name'] = contributor['enterprisename']

                        for question in contributor['responsesByReferenceAndPeriodAndSurvey']['nodes']:
                            if question['questioncode'] in question_codes:
                                outContrib[question_labels[question['questioncode']]] = question['response']

                        if contributor['survey'] == "066":
                            outContrib['land_or_marine'] = "L"
                        elif contributor['survey'] == "076":
                            outContrib['land_or_marine'] = "M"

                        output_json.append(outContrib)

        s3.Object(bucket_name, "test_results_ingest_output.json").put(Body=output_json)

    except AttributeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": 0}


def read_from_s3(bucket_name, file_name):
    """
    Given the name of the bucket and the filename(key), this function will
    return a file. File is JSON format.
    :param bucket_name: Name of the S3 bucket - Type: String
    :param file_name: Name of the file - Type: String
    :return: input_file: The JSON file in S3 - Type: JSON
    """
    object = s3.Object(bucket_name, file_name)
    input_file = object.get()['Body'].read()

    return input_file


def send_sns_message(checkpoint, sns_topic_arn):
    """
    This method is responsible for sending a notification to the specified arn,
    so that it can be used to relay information for the BPM to use and handle.
    :param checkpoint: The current checkpoint location - Type: String.
    :param sns_topic_arn: The arn of the sns topic you are directing the message at -
                          Type: String.
    :return: None
    """
    sns_message = {
        "success": True,
        "module": "Results Data Ingest",
        "checkpoint": checkpoint,
        "message": ""
    }

    return sns.publish(TargetArn=sns_topic_arn, Message=json.dumps(sns_message))
