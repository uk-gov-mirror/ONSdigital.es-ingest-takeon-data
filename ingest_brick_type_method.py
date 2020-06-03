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

        input_json = event['RuntimeVariables']['data']
        output_json = []

        # TODO
        # for each respondent
        #   generate all brick questions with 0s
        #   get the brick type of responder
        #   replace this type with the values from the 'shared colum'
        #   remove the 'shared column'

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
