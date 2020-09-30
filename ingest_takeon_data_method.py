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
    data = fields.Dict(required=True)
    period = fields.Str(required=True)
    periodicity = fields.Str(required=True)
    question_labels = fields.Dict(required=True)
    survey_codes = fields.Dict(required=True)
    statuses = fields.Dict(required=True)


def lambda_handler(event, context):
    """
    This method will ingest data from Take On S3 bucket, transform it so that it fits
    in the results pipeline, and send it to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: Dict with "success" and "data" or "success and "error".
    """
    current_module = "Results Ingest - Takeon Data - Method"
    error_message = ""
    logger = general_functions.get_logger()
    # Define run_id outside of try block.

    bpm_queue_url = None

    run_id = 0
    try:
        logger.info("Retrieving data from take on file.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling.
        run_id = event["RuntimeVariables"]["run_id"]

        # Extract runtime variables.
        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])
        logger.info("Validated parameters.")

        bpm_queue_url = runtime_variables["bpm_queue_url"]
        period = runtime_variables["period"]
        periodicity = runtime_variables["periodicity"]
        previous_period = general_functions.calculate_adjacent_periods(period,
                                                                       periodicity)
        question_labels = runtime_variables["question_labels"]
        survey_codes = runtime_variables["survey_codes"]
        statuses = runtime_variables["statuses"]
        input_json = runtime_variables["data"]

        logger.info("Retrieved configuration variables.")

        output_json = []

        for survey in input_json["data"]["allSurveys"]["nodes"]:
            if survey["survey"] in survey_codes:
                for contributor in survey["contributorsBySurvey"]["nodes"]:
                    if contributor["period"] in (period, previous_period):
                        out_contrib = {}

                        # Basic contributor information.
                        out_contrib["survey"] = survey_codes[contributor["survey"]]
                        out_contrib["period"] = str(contributor["period"])
                        out_contrib["responder_id"] = str(contributor["reference"])
                        out_contrib["gor_code"] = contributor["region"]
                        out_contrib["enterprise_reference"] = str(
                            contributor["enterprisereference"])
                        out_contrib["enterprise_name"] = contributor["enterprisename"]

                        # Pre-populate default question answers.
                        for expected_question in question_labels.keys():
                            out_contrib[question_labels[expected_question]] = 0

                        # Where contributors provided an aswer, use it instead.
                        for question in contributor["responsesByReferenceAndPeriodAndSurvey"]["nodes"]:  # noqa: E501
                            if question["questioncode"] in question_labels.keys() and\
                               question["response"].isnumeric():
                                out_contrib[question_labels[question["questioncode"]]]\
                                    = int(question["response"])

                        # Convert the response statuses to types,
                        # used by results to check if imputation should run
                        # assume all unknown statuses need to be imputed
                        # (this may change after further cross-team talks).
                        if contributor["status"] in statuses:
                            out_contrib["response_type"] = statuses[contributor["status"]]
                        else:
                            out_contrib["response_type"] = 1

                        output_json.append(out_contrib)

        logger.info("Successfully extracted data from take on.")
        final_output = {"data": json.dumps(output_json)}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id, context=context, bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output
