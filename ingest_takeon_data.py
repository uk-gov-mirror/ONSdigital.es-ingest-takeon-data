import json
import logging
import os

import boto3
import marshmallow
from botocore.exceptions import ClientError, IncompleteReadError, ParamValidationError

# boto3 clients
s3 = boto3.resource('s3', region_name='eu-west-2')
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')


class InputSchema(marshmallow.Schema):
    """
    Scheme to ensure that environment variables are present and in the correct format.
    :return: None
    """
    takeon_bucket_name = marshmallow.fields.Str(required=True)
    results_bucket_name = marshmallow.fields.Str(required=True)
    file_name = marshmallow.fields.Str(required=True)
    function_name = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    sqs_queue_url = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)
    sns_topic_arn = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method will ingest data from Take On S3 bucket, transform it so that it fits
    in the results pipeline, and send it to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: Success - True/False & Checkpoint
    """
    current_module = "BMI Results Data Ingest - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Results Data Ingest - Wrangler")
    logger.setLevel(10)
    try:
        logger.info("Running Results Data Ingest...")

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client('lambda', region_name='eu-west-2')

        # ENV vars
        config, errors = InputSchema().load(os.environ)
        takeon_bucket_name = config['takeon_bucket_name']
        results_bucket_name = config['results_bucket_name']
        file_name = config['file_name']
        function_name = config['function_name']
        checkpoint = config['checkpoint']
        sqs_queue_url = config['sqs_queue_url']  # noqa
        sqs_messageid_name = config['sqs_messageid_name']  # noqa
        sns_topic_arn = config['sns_topic_arn']  # noqa

        logger.info("Validated environment parameters.")

        input_file = read_from_s3(takeon_bucket_name, file_name)

        logger.info("Read from S3.")

        method_return = lambda_client.invoke(
         FunctionName=function_name, Payload=input_file
        )

        output_json = method_return.get('Payload').read().decode("utf-8")

        write_to_s3(
            results_bucket_name,
            "test_results_ingest_output.json",
            json.loads(output_json)
        )

        logger.info("Data ready for Results pipeline. Written to S3.")

        send_sns_message(checkpoint, sns_topic_arn)

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str("aws_request_id"))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str("aws_request_id"))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str("aws_request_id"))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ParamValidationError as e:
        error_message = ("Blank or empty environment variable in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str("aws_request_id"))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str("aws_request_id"))

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
    :param file_name: Name of the file to be read - Type: String
    :return: input_file: The JSON file in S3 - Type: JSON
    """
    object = s3.Object(bucket_name, file_name)
    input_file = object.get()['Body'].read()

    return input_file


def write_to_s3(bucket_name, file_name, input_body):
    """
    Given the bucket name, desired file name and file body, writes a file to the
    specified bucket.
    :param bucket_name: Name of the S3 bucket - Type: String
    :param file_name: Name of the file to be written - Type: String
    :param input_body: Data to be written to the file - Type: JSON
    :return: None
    """
    s3.Object(bucket_name, file_name).put(
        Body=json.dumps(input_body)
    )


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
        "message": "Take On data ingest successful. Output file stored in " +
        "results-s3-bucket as results_ingest_output.json"
    }

    return sns.publish(TargetArn=sns_topic_arn, Message=json.dumps(sns_message))
