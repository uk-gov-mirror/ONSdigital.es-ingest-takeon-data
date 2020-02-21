import json
import logging

from es_aws_functions import general_functions


def lambda_handler(event, context):
    """
    This method will ingest data from Take On S3 bucket, transform it so that it fits
    in the results pipeline, and send it to the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: Dict with "success" and "data" or "success and "error".
    """
    current_module = "Results Data Ingest - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Results Data Ingest")
    logger.setLevel(10)
    try:
        logger.info("Retrieving data from take on file...")

        period = event['period']
        periodicity = event['periodicity']
        previous_period = general_functions.calculate_adjacent_periods(period,
                                                                       periodicity)

        question_labels = {
            '0601': 'Q601_asphalting_sand',
            '0602': 'Q602_building_soft_sand',
            '0603': 'Q603_concreting_sand',
            '0604': 'Q604_bituminous_gravel',
            '0605': 'Q605_concreting_gravel',
            '0606': 'Q606_other_gravel',
            '0607': 'Q607_constructional_fill',
            '0608': 'Q608_total'
        }

        input_json = event['data']
        output_json = []
        for survey in input_json['data']['allSurveys']['nodes']:
            if survey['survey'] == "0066" or survey['survey'] == "0076":
                for contributor in survey['contributorsBySurvey']['nodes']:
                    if contributor['period'] in (period, previous_period):
                        out_contrib = {}
                        # basic contributor information
                        if (contributor['survey'] == "0066"):
                            out_contrib['survey'] = "066"
                        else:
                            out_contrib['survey'] = "076"
                        out_contrib['period'] = str(contributor['period'])
                        out_contrib['responder_id'] = str(contributor['reference'])
                        out_contrib['gor_code'] = contributor['region']
                        out_contrib['enterprise_reference'] = str(
                            contributor['enterprisereference'])
                        out_contrib['enterprise_name'] = contributor['enterprisename']

                        # prepopulate default question answers
                        for expected_question in question_labels.keys():
                            out_contrib[question_labels[expected_question]] = 0

                        # will mark if there is at least one actual response
                        data_provided = False

                        # where contributors provided an aswer, use it instead
                        for question in contributor['responsesByReferenceAndPeriodAndSurvey']['nodes']:  # noqa: E501
                            if question['questioncode'] in question_labels.keys() and\
                               question['response'].isnumeric():
                                out_contrib[question_labels[question['questioncode']]]\
                                    = int(question['response'])

                                # check if response was provided
                                if int(question['response']) != 0:
                                    data_provided = True

                        # response type indicator/status to be decided between the teams
                        # this should work for now
                        if data_provided:
                            out_contrib['response_type'] = 2
                        else:
                            out_contrib['response_type'] = 1

                        output_json.append(out_contrib)

        logger.info("Successfully extracted data from take on.")
        final_output = {"data": json.dumps(output_json)}

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

    logger.info("Successfully completed method: " + current_module)
    final_output["success"] = True
    return final_output
