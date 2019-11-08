import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError, ParamValidationError
from esawsfunctions import funk
from marshmallow import Schema, fields


class InputSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    These vairables are expected by the method, and it will fail to run if not provided.
    :return: None
    """
    checkpoint = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    results_bucket_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    takeon_bucket_name = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method will ingest data from Take On S3 bucket, transform it so that it fits
    in the results pipeline, and send it to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: JSON String - {"success": boolean, "checkpoint"/"error": integer/string}
    """
    current_module = "Results Data Ingest - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Results Data Ingest")
    logger.setLevel(10)
    try:
        logger.info("Running Results Data Ingest...")

        lambda_client = boto3.client('lambda', region_name='eu-west-2')

        # ENV vars
        config, errors = InputSchema().load(os.environ)
        checkpoint = config['checkpoint']
        in_file_name = config['in_file_name']
        method_name = config['method_name']
        out_file_name = config['out_file_name']
        queue_url = config['queue_url']
        results_bucket_name = config['results_bucket_name']
        sns_topic_arn = config['sns_topic_arn']
        sqs_messageid_name = config['sqs_messageid_name']
        takeon_bucket_name = config['takeon_bucket_name']

        logger.info("Validated environment parameters.")

        input_file = funk.read_from_s3(takeon_bucket_name, in_file_name)

        logger.info("Read from S3.")

        method_return = lambda_client.invoke(
         FunctionName=method_name, Payload=input_file
        )

        output_json = json.loads(method_return.get('Payload').read().decode("utf-8"))

        funk.save_data(results_bucket_name, out_file_name,
                       output_json, queue_url, sqs_messageid_name)

        logger.info("Data ready for Results pipeline. Written to S3.")

        funk.send_sns_message(checkpoint, sns_topic_arn, "Ingest has succeeded.")

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

    except ParamValidationError as e:
        error_message = ("Blank or empty environment variable in "
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
