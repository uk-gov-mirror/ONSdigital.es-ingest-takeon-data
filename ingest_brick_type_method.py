import json
import logging

from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields


class RuntimeSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    data = fields.List(fields.Dict(required=True))
    brick_questions = fields.Dict(required=True)
    brick_types = fields.List(fields.Int(required=True))
    brick_type_column = fields.Str(required=True)
    environment = fields.Str(required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method will take the simple bricks survey data and expand it to have seperate
     coloumn for each brick type as expceted by the results pipeline. It'll then send it
     to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: Dict with "success" and "data" or "success and "error".
    """
    current_module = "Results Ingest - Brick Type - Method"
    error_message = ""
    # Variables required for error handling.
    run_id = 0
    bpm_queue_url = None
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']
        # Extract runtime variables.
        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        bpm_queue_url = runtime_variables["bpm_queue_url"]
        brick_questions = runtime_variables['brick_questions']
        brick_types = runtime_variables['brick_types']
        brick_type_column = runtime_variables['brick_type_column']
        data = runtime_variables['data']
        environment = runtime_variables["environment"]
        survey = runtime_variables["survey"]
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
        return {"success": False, "error": error_message}

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
        return {"success": False, "error": error_message}

    try:
        logger.info("Started - retrieved wrangler configuration variables.")
        # Apply changes to every responder and every brick type
        for respondent in data:
            for this_type in brick_types:

                # When it's not the brick type this responder supplied, fill with 0s.
                if respondent[brick_type_column] != this_type:
                    for this_question in brick_questions[str(this_type)]:
                        respondent[brick_questions[str(this_type)][this_question]] = 0

                # When it's the same as responder supplied, use their data.
                else:
                    for this_question in brick_questions[str(this_type)]:
                        respondent[brick_questions[str(this_type)][this_question]] = \
                            respondent[this_question]

            # Remove the 'shared' questions for respondents
            if str(respondent[brick_type_column]) in str(brick_types):
                for this_question in brick_questions[str(respondent[brick_type_column])]:
                    respondent.pop(this_question, None)
            # Remove the 'shared' question for non-respondents
            else:
                for this_question in next(iter(brick_questions)):
                    respondent.pop(this_question, None)

        logger.info("Successfully expanded brick data.")
        final_output = {"data": json.dumps(data)}
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module.")
    final_output['success'] = True
    return final_output
