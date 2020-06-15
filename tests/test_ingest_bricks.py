import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

import ingest_brick_type_method as lambda_method_function
import ingest_brick_type_wrangler as lambda_wrangler_function

wrangler_environment_variables = {
                "takeon_bucket_name": "test_bucket",
                "results_bucket_name": "test_bucket",
                # bucket_name included for test library to use:
                "bucket_name": "test_bucket",
                "method_name": "mock-function",
                "checkpoint": "0"
            }

wrangler_runtime_variables = {"RuntimeVariables": {
    "run_id": "bob",
    "in_file_name": "mock-file",
    "out_file_name": "test_wrangler_prepared_output.json",
    "outgoing_message_group_id": "mock_out_group",
    "sns_topic_arn": "mock-topic-arn",
    "sqs_queue_url": "mock-sqs-url",
    "location": "Here",
    "ingestion_parameters": {
        "question_labels": {
            '0001': 'opening_stock_commons',
            '0011': 'opening_stock_facings',
            '0021': 'opening_stock_engineering',
            '0002': 'produced_commons',
            '0012': 'produced_facings',
            '0022': 'produced_engineering',
            '0003': 'deliveries_commons',
            '0013': 'deliveries_facings',
            '0023': 'deliveries_engineering',
            '0004': 'closing_stock_commons',
            '0014': 'closing_stock_facings',
            '0024': 'closing_stock_engineering',
            '0501': 'total_opening_stock',
            '0502': 'total_produced',
            '0503': 'total_deliveries',
            '0504': 'total_closing',
            '8000': 'brick_type'
        },
        "survey_codes": {
            "0074": "047"
        },
        "statuses": {
            "Form Sent Out": 1,
            "Clear": 2,
            "Overridden": 2
        },
        "brick_types": [
            2,
            3,
            4
        ],
        "brick_type_column": "brick_type",
        "brick_questions": {
            2: {
                'opening_stock_commons': "clay_opening_stock_commons",
                'opening_stock_facings': "clay_opening_stock_facings",
                'opening_stock_engineering': "clay_opening_stock_engineering",
                'produced_commons': "clay_produced_commons",
                'produced_facings': "clay_produced_facings",
                'produced_engineering': "clay_produced_engineering",
                'deliveries_commons': "clay_deliveries_commons",
                'deliveries_facings': "clay_deliveries_facings",
                'deliveries_engineering': "clay_deliveries_engineering",
                'closing_stock_commons': "clay_closing_stock_commons",
                'closing_stock_facings': "clay_closing_stock_facings",
                'closing_stock_engineering': "clay_closing_stock_engineering"
            },
            3: {
                'opening_stock_commons': "concrete_opening_stock_commons",
                'opening_stock_facings': "concrete_opening_stock_facings",
                'opening_stock_engineering': "concrete_opening_stock_engineering",
                'produced_commons': "concrete_produced_commons",
                'produced_facings': "concrete_produced_facings",
                'produced_engineering': "concrete_produced_engineering",
                'deliveries_commons': "concrete_deliveries_commons",
                'deliveries_facings': "concrete_deliveries_facings",
                'deliveries_engineering': "concrete_deliveries_engineering",
                'closing_stock_commons': "concrete_closing_stock_commons",
                'closing_stock_facings': "concrete_closing_stock_facings",
                'closing_stock_engineering': "concrete_closing_stock_engineering"
            },
            4: {
                'opening_stock_commons': "sandlime_opening_stock_commons",
                'opening_stock_facings': "sandlime_opening_stock_facings",
                'opening_stock_engineering': "sandlime_opening_stock_engineering",
                'produced_commons': "sandlime_produced_commons",
                'produced_facings': "sandlime_produced_facings",
                'produced_engineering': "sandlime_produced_engineering",
                'deliveries_commons': "sandlime_deliveries_commons",
                'deliveries_facings': "sandlime_deliveries_facings",
                'deliveries_engineering': "sandlime_deliveries_engineering",
                'closing_stock_commons': "sandlime_closing_stock_commons",
                'closing_stock_facings': "sandlime_closing_stock_facings",
                'closing_stock_engineering': "sandlime_closing_stock_engineering"
            }
        }
    }
}}


method_runtime_variables = {
    "RuntimeVariables": {
        "data": {},
        "run_id": "bob",
        "question_labels": {
            '0001': 'opening_stock_commons',
            '0011': 'opening_stock_facings',
            '0021': 'opening_stock_engineering',
            '0002': 'produced_commons',
            '0012': 'produced_facings',
            '0022': 'produced_engineering',
            '0003': 'deliveries_commons',
            '0013': 'deliveries_facings',
            '0023': 'deliveries_engineering',
            '0004': 'closing_stock_commons',
            '0014': 'closing_stock_facings',
            '0024': 'closing_stock_engineering',
            '0501': 'total_opening_stock',
            '0502': 'total_produced',
            '0503': 'total_deliveries',
            '0504': 'total_closing',
            '8000': 'brick_type'
        },
        "survey_codes": {
            "0074": "047"
        },
        "statuses": {
            "Form Sent Out": 1,
            "Clear": 2,
            "Overridden": 2
        },
        "brick_types": [
            2,
            3,
            4
        ],
        "brick_type_column": "brick_type",
        "brick_questions": {
            2: {
                'opening_stock_commons': "clay_opening_stock_commons",
                'opening_stock_facings': "clay_opening_stock_facings",
                'opening_stock_engineering': "clay_opening_stock_engineering",
                'produced_commons': "clay_produced_commons",
                'produced_facings': "clay_produced_facings",
                'produced_engineering': "clay_produced_engineering",
                'deliveries_commons': "clay_deliveries_commons",
                'deliveries_facings': "clay_deliveries_facings",
                'deliveries_engineering': "clay_deliveries_engineering",
                'closing_stock_commons': "clay_closing_stock_commons",
                'closing_stock_facings': "clay_closing_stock_facings",
                'closing_stock_engineering': "clay_closing_stock_engineering"
            },
            3: {
                'opening_stock_commons': "concrete_opening_stock_commons",
                'opening_stock_facings': "concrete_opening_stock_facings",
                'opening_stock_engineering': "concrete_opening_stock_engineering",
                'produced_commons': "concrete_produced_commons",
                'produced_facings': "concrete_produced_facings",
                'produced_engineering': "concrete_produced_engineering",
                'deliveries_commons': "concrete_deliveries_commons",
                'deliveries_facings': "concrete_deliveries_facings",
                'deliveries_engineering': "concrete_deliveries_engineering",
                'closing_stock_commons': "concrete_closing_stock_commons",
                'closing_stock_facings': "concrete_closing_stock_facings",
                'closing_stock_engineering': "concrete_closing_stock_engineering"
            },
            4: {
                'opening_stock_commons': "sandlime_opening_stock_commons",
                'opening_stock_facings': "sandlime_opening_stock_facings",
                'opening_stock_engineering': "sandlime_opening_stock_engineering",
                'produced_commons': "sandlime_produced_commons",
                'produced_facings': "sandlime_produced_facings",
                'produced_engineering': "sandlime_produced_engineering",
                'deliveries_commons': "sandlime_deliveries_commons",
                'deliveries_facings': "sandlime_deliveries_facings",
                'deliveries_engineering': "sandlime_deliveries_engineering",
                'closing_stock_commons': "sandlime_closing_stock_commons",
                'closing_stock_facings': "sandlime_closing_stock_facings",
                'closing_stock_engineering': "sandlime_closing_stock_engineering"
            }
        }
    }
}
##########################################################################################
#                                     Generic                                            #
##########################################################################################


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
def test_client_error(which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_method_function, method_runtime_variables,
         [], "ingest_takeon_data_method.general_functions.calculate_adjacent_periods",
         "'Exception'", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, "ingest_takeon_data_wrangler.EnvironmentSchema",
         "'Exception'", test_generic_library.wrangler_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
@mock.patch('ingest_takeon_data_wrangler.aws_functions.read_from_s3',
            return_value=json.dumps({"test": "test"}))
def test_incomplete_read_error(mock_s3_get):
    file_list = ["test_ingest_input.json"]
    test_generic_library.incomplete_read_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "ingest_takeon_data_wrangler",
                                               "IncompleteReadError")


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_method_function, {},
         "KeyError", test_generic_library.method_assert),
        (lambda_wrangler_function, wrangler_environment_variables,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@mock_s3
@mock.patch('ingest_takeon_data_wrangler.aws_functions.read_from_s3',
            return_value=json.dumps({"test": "test"}))
def test_method_error(mock_s3_get):
    file_list = ["test_ingest_input.json"]

    test_generic_library.wrangler_method_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "ingest_takeon_data_wrangler")


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [
        (lambda_method_function, "Error validating runtime params",
         test_generic_library.method_assert, {}),
        (lambda_wrangler_function, "Error validating environment params",
         test_generic_library.wrangler_assert, {})])
def test_value_error(which_lambda, expected_message, assertion,
                     which_environment_variables):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion,
        environment_variables=which_environment_variables)


##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
def test_method_success():
    """
    Runs the method function.
    :param None
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_method_prepared_output.json", "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open("tests/fixtures/test_ingest_input.json", "r") as file_2:
        test_data = file_2.read()
    method_runtime_variables["RuntimeVariables"]["data"] = json.loads(test_data)

    output = lambda_method_function.lambda_handler(
        method_runtime_variables, test_generic_library.context_object)

    produced_data = pd.DataFrame(json.loads(output["data"]))

    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@mock.patch('ingest_takeon_data_wrangler.aws_functions.read_from_s3')
def test_wrangler_success_passed(mock_s3_get):
    """
    Runs the wrangler function.
    :param mock_s3_get - Replacement Function For The Data Retrieval AWS Functionality.
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_ingest_input.json", "r") as file:
        wrangler_input = json.dumps(file.read())
    mock_s3_get.return_value = wrangler_input
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_ingest_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("ingest_takeon_data_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            # Rather than mock the get/decode we tell the code that when the invoke is
            # called pass the variables to this replacement function instead.
            mock_client_object.invoke.side_effect =\
                test_generic_library.replacement_invoke

            # This stops the Error caused by the replacement function from stopping
            # the test.
            with pytest.raises(exception_classes.LambdaFailure):
                lambda_wrangler_function.lambda_handler(
                    wrangler_runtime_variables, test_generic_library.context_object
                )

    with open("tests/fixtures/test_ingest_input.json", "r") as file_2:
        test_data_prepared = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/test_wrangler_to_method_input.json", "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    # Compares the data.
    assert_frame_equal(produced_data, prepared_data)

    with open("tests/fixtures/test_wrangler_to_method_runtime.json", "r") as file_4:
        test_dict_prepared = file_4.read()
    produced_dict = json.loads(test_dict_prepared)

    # Ensures data is not in the RuntimeVariables and then compares.
    method_runtime_variables["RuntimeVariables"]["data"] = None
    assert produced_dict == method_runtime_variables["RuntimeVariables"]


@mock_s3
@mock.patch('ingest_takeon_data_wrangler.aws_functions.read_from_s3')
@mock.patch('ingest_takeon_data_wrangler.aws_functions.save_data',
            side_effect=test_generic_library.replacement_save_data)
def test_wrangler_success_returned(mock_s3_put, mock_s3_get):
    """
    Runs the wrangler function after the method invoke.
    :param None
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_ingest_input.json", "r") as file:
        wrangler_input = json.dumps(file.read())
    mock_s3_get.return_value = wrangler_input
    with open("tests/fixtures/test_method_prepared_output.json", "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("ingest_takeon_data_wrangler.boto3.client") as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            mock_client_object.invoke.return_value.get.return_value.read \
                .return_value.decode.return_value = json.dumps({
                 "data": test_data_out,
                 "success": True,
                 "anomalies": []
                })

            output = lambda_wrangler_function.lambda_handler(
                wrangler_runtime_variables, test_generic_library.context_object
            )

    with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_3:
        test_data_prepared = file_3.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))
    with open("tests/fixtures/" +
              wrangler_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_4:
        test_data_produced = file_4.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)
