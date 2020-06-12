import json
import logging

from es_aws_functions import general_functions


def lambda_handler(event, context):
    """
    This method will take the simple bricks survey data and expand it to have seperate
     coloumn for each brick type as expceted by the results pipeline. It'll then send it to
     the Results S3 bucket for further processing.
    :param event: Event object
    :param context: Context object
    :return: Dict with "success" and "data" or "success and "error".
    """
    current_module = "Results Ingest - Brick Type - Method"
    error_message = ""
    logger = logging.getLogger("Results Ingest - Brick Type")
    logger.setLevel(10)
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Retrieving data from take on file...")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        # Extract configuration variables
        brick_questions = event['RuntimeVariables']['brick_questions']
        brick_types = event['RuntimeVariables']['brick_types']
        brick_type_column = event['RuntimeVariables']['brick_type_column']

        input_json = event['RuntimeVariables']['data']
        output_json = []

        # Apply changes to every responder and every brick type
        for respondent in input_json:
            for this_type in brick_types:

                # When it's not the brick type this responder supplied, fill with 0s.
                if respondent[brick_type_column] != this_type:
                    for this_question in brick_questions:
                        respondent[brick_questions[this_question]] = 0

                # When it's the same as responder supplied, use their data.
                else:
                    for this_question in brick_questions:
                        respondent[brick_questions[this_question]] =\
                             respondent[this_question]

            # Remove the 'shared' questions.
            for this_question in brick_questions:
                respondent.pop(this_question, None)

        logger.info("Successfully expanded brick data.")
        final_output = {"data": json.dumps(output_json)}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    return final_output
