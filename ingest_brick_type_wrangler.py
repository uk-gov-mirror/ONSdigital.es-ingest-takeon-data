import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    results_bucket_name = fields.Str(required=True)


class IngestionParamsSchema(Schema):

    question_labels = fields.Dict(required=True)
    survey_codes = fields.Dict(required=True)
    statuses = fields.Dict(required=True)


class RuntimeSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    in_file_name = fields.Str(required=True)
    ingestion_parameters = fields.Nested(IngestionParamsSchema, required=True)
    location = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    outgoing_message_group_id = fields.Str(required=True)
    period = fields.Str(required=True)
    periodicity = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method will take the simple bricks survey data and expand it to have seperate
     coloumn for each brick type as expceted by the results pipeline. It'll then send it to
     the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: JSON String - {"success": boolean, "checkpoint"/"error": integer/string}
    """
    current_module = "Results Ingest - Brick Type - Wrangler"
    error_message = ""
    logger = logging.getLogger("Results Ingest - Brick Type")
    logger.setLevel(10)

    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Load variables.
        environment_variables = EnvironmentSchema().load(os.environ)
        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])
        logger.info("Validated parameters.")

        # Environment Variables
        checkpoint = environment_variables["checkpoint"]
        method_name = environment_variables["method_name"]
        results_bucket_name = environment_variables["results_bucket_name"]

        # Runtime Variables
        in_file_name = runtime_variables["in_file_name"]
        location = runtime_variables["location"]
        out_file_name = runtime_variables["out_file_name"]
        outgoing_message_group_id = runtime_variables["outgoing_message_group_id"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        sqs_queue_url = runtime_variables["queue_url"]
        ingestion_parameters = event["RuntimeVariables"]["ingestion_parameters"]

        logger.info("Validated environment parameters.")
        # Set up client.
        lambda_client = boto3.client("lambda", region_name="eu-west-2")
        input_file = aws_functions.read_from_s3(results_bucket_name,
                                                in_file_name,
                                                file_extension="")

        logger.info("Read from S3.")

        payload = {

            "RuntimeVariables": {
                "data": json.loads(input_file),
                "run_id": run_id,
                "brick_questions": ingestion_parameters["brick_questions"],
                "brick_types": ingestion_parameters["brick_types"]
            },
        }

        method_return = lambda_client.invoke(
         FunctionName=method_name, Payload=json.dumps(payload)
        )
        logger.info("Successfully invoked method.")

        json_response = json.loads(method_return.get("Payload").read().decode("utf-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        aws_functions.save_data(results_bucket_name, out_file_name,
                                json_response["data"], sqs_queue_url,
                                outgoing_message_group_id, location)

        logger.info("Data ready for Results pipeline. Written to S3.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn, "Ingest.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
