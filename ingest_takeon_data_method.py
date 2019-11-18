import logging
import os

import marshmallow


class InputSchema(marshmallow.Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    These vairables are expected by the method, and it will fail to run if not provided.
    :return: None
    """
    period = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method will ingest data from Take On S3 bucket, transform it so that it fits
    in the results pipeline, and send it to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: JSON String - {"success": boolean, "checkpoint"/"error": integer/string}
    """
    current_module = "Results Data Ingest - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Results Data Ingest")
    logger.setLevel(10)
    try:
        logger.info("Retrieving data from take on file...")

        config, errors = InputSchema().load(os.environ)

        period = config['period']
        question_labels = {
            '601': 'Q601_asphalting_sand',
            '602': 'Q602_building_soft_sand',
            '603': 'Q603_concreting_sand',
            '604': 'Q604_bituminous_gravel',
            '605': 'Q605_concreting_gravel',
            '606': 'Q606_other_gravel',
            '607': 'Q607_constructional_fill',
            '608': 'Q608_total'
        }

        input_json = event
        output_json = []
        for survey in input_json['data']['allSurveys']['nodes']:
            if survey['survey'] == "066" or survey['survey'] == "076":
                for contributor in survey['contributorsBySurvey']['nodes']:
                    if contributor['period'] == period:
                        out_contrib = {}
                        # basic contributor information
                        out_contrib['period'] = contributor['period']
                        out_contrib['responder_id'] = contributor['reference']
                        out_contrib['gor_code'] = contributor['region']
                        # Not Known If Below Is Correct Column.
                        # Temp Transform To Use As Response Type.
                        if contributor['formid'] == 1:
                            out_contrib['response_type'] = 2
                        else:
                            out_contrib['response_type'] = 1
                        out_contrib['enterprise_ref'] = contributor['enterprisereference']
                        out_contrib['name'] = contributor['enterprisename']

                        # prepopulate default question answers
                        for expected_question in question_labels.keys():
                            out_contrib[question_labels[expected_question]] = 0

                        # where contributors provided an aswer, use it instead
                        for question in contributor['responsesByReferenceAndPeriodAndSurvey']['nodes']:  # noqa: E501
                            if question['questioncode'] in question_labels.keys():
                                out_contrib[question_labels[question['questioncode']]]\
                                    = int(question['response'])

                        # survey marker is used instead of the survey code
                        # -------------------------------------------- #
                        # Do We Need To Do This Or Should Use
                        # The Survey Code Throughout Our Processes.
                        # This Would Allow Us To Make Other Modules
                        # More Generic.
                        # -------------------------------------------- #
                        if contributor['survey'] == "066":
                            out_contrib['land_or_marine'] = "L"
                        elif contributor['survey'] == "076":
                            out_contrib['land_or_marine'] = "M"

                        output_json.append(out_contrib)

        logger.info("Successfully extracted data from take on.")

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed method: " + current_module)
            return output_json
