import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.testing import assert_frame_equal

import ingest_brick_type_method as lambda_method_function_bricks
import ingest_brick_type_wrangler as lambda_wrangler_function_bricks
import ingest_takeon_data_method as lambda_method_function_data
import ingest_takeon_data_wrangler as lambda_wrangler_function_data

wrangler_environment_variables = {
                "results_bucket_name": "test_bucket",
                # bucket_name included for test library to use:
                "bucket_name": "test_bucket",
                "method_name": "mock-function"
            }

wrangler_runtime_variables_data = {"RuntimeVariables": {
    "bpm_queue_url": "fake_queue_url",
    "total_steps": "6",
    "run_id": "bob",
    "snapshot_s3_uri": "s3://test_bucket/test_ingest_input.json",
    "out_file_name": "test_wrangler_prepared_output.json",
    "period": "201809",
    "periodicity": "03",
    "sns_topic_arn": "mock-topic-arn",
    "ingestion_parameters": {
        "question_labels": {
            "0601": "Q601_asphalting_sand",
            "0602": "Q602_building_soft_sand",
            "0603": "Q603_concreting_sand",
            "0604": "Q604_bituminous_gravel",
            "0605": "Q605_concreting_gravel",
            "0606": "Q606_other_gravel",
            "0607": "Q607_constructional_fill",
            "0608": "Q608_total"
        },
        "survey_codes": {
            "0066": "066",
            "0076": "076"
        },
        "statuses": {
            "Form Sent Out": 1,
            "Clear": 2,
            "Overridden": 2
        }
    }
}}

wrangler_runtime_variables_bricks = {"RuntimeVariables": {
    "bpm_queue_url": "fake_bpm_queue",
    "total_steps": "6",
    "run_id": "bob",
    "in_file_name": "test_bricks_method_input",
    "out_file_name": "test_bricks_wrangler_prepared_output.json",
    "sns_topic_arn": "mock-topic-arn",
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
        },
    }
}}

method_runtime_variables_data = {
    "RuntimeVariables": {
        "data": {},
        "bpm_queue_url": "fake_queue_url",
        "period": "201809",
        "periodicity": "03",
        "run_id": "bob",
        "question_labels": {
            "0601": "Q601_asphalting_sand",
            "0602": "Q602_building_soft_sand",
            "0603": "Q603_concreting_sand",
            "0604": "Q604_bituminous_gravel",
            "0605": "Q605_concreting_gravel",
            "0606": "Q606_other_gravel",
            "0607": "Q607_constructional_fill",
            "0608": "Q608_total"
        },
        "survey_codes": {
            "0066": "066",
            "0076": "076"
        },
        "statuses": {
            "Form Sent Out": 1,
            "Clear": 2,
            "Overridden": 2
        }
    }
}

method_runtime_variables_bricks = {
    "RuntimeVariables": {
        "data": {},
        "bpm_queue_url": "fake_queue_url",
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
            "2": {
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
            "3": {
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
            "4": {
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
        (lambda_wrangler_function_data, wrangler_runtime_variables_data,
         wrangler_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_wrangler_function_bricks, wrangler_runtime_variables_bricks,
         wrangler_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
@mock.patch('ingest_takeon_data_wrangler.aws_functions.send_bpm_status')
def test_client_error(send_bpm_status, which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_method_function_data, method_runtime_variables_data,
         [], "ingest_takeon_data_method.general_functions.calculate_adjacent_periods",
         "'Exception'", test_generic_library.method_assert),
        (lambda_wrangler_function_data, wrangler_runtime_variables_data,
         wrangler_environment_variables, "ingest_takeon_data_wrangler.EnvironmentSchema",
         "'Exception'", test_generic_library.wrangler_assert),
        (lambda_method_function_bricks, method_runtime_variables_bricks,
         [], "ingest_brick_type_method.RuntimeSchema",
         "'Exception'", test_generic_library.method_assert),
        (lambda_wrangler_function_bricks, wrangler_runtime_variables_bricks,
         wrangler_environment_variables, "ingest_brick_type_wrangler.EnvironmentSchema",
         "'Exception'", test_generic_library.wrangler_assert)
    ])
@mock.patch('ingest_takeon_data_wrangler.aws_functions.send_bpm_status')
def test_general_error(send_bpm_status, which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
@pytest.mark.parametrize(
     "which_method,which_wrangler,which_environment_variables,which_runtime_variables," +
     "which_file_list",
     [
        (lambda_wrangler_function_data, "ingest_takeon_data_wrangler",
         wrangler_environment_variables, wrangler_runtime_variables_data,
         "test_ingest_input.json"),
        (lambda_wrangler_function_bricks, "ingest_brick_type_wrangler",
         wrangler_environment_variables, wrangler_runtime_variables_bricks,
         "test_bricks_method_input.json")
     ]
)
def test_incomplete_read_error(which_method, which_wrangler, which_environment_variables,
                               which_runtime_variables, which_file_list):
    file_list = [which_file_list]
    test_generic_library.incomplete_read_error(which_method,
                                               which_runtime_variables,
                                               which_environment_variables,
                                               file_list,
                                               which_wrangler,
                                               "IncompleteReadError")


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_method_function_data, {},
         "KeyError", test_generic_library.method_assert),
        (lambda_wrangler_function_data, wrangler_environment_variables,
         "KeyError", test_generic_library.wrangler_assert),
        (lambda_method_function_bricks, {},
         "KeyError", test_generic_library.method_assert),
        (lambda_wrangler_function_bricks, wrangler_environment_variables,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@mock_s3
@pytest.mark.parametrize(
    "which_method,which_wrangler,which_environment_variables,which_runtime_variables," +
    "which_file_list",
    [
        (lambda_wrangler_function_data, "ingest_takeon_data_wrangler",
         wrangler_environment_variables, wrangler_runtime_variables_data,
         "test_ingest_input.json"),
        (lambda_wrangler_function_bricks, "ingest_brick_type_wrangler",
         wrangler_environment_variables, wrangler_runtime_variables_bricks,
         "test_bricks_method_input.json")
    ]
)
def test_method_error(which_method, which_wrangler, which_environment_variables,
                      which_runtime_variables, which_file_list):
    file_list = [which_file_list]

    test_generic_library.wrangler_method_error(which_method,
                                               which_runtime_variables,
                                               which_environment_variables,
                                               file_list,
                                               which_wrangler)


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [
        (lambda_method_function_data, "Error validating runtime params",
         test_generic_library.method_assert, {}),
        (lambda_wrangler_function_data, "Error validating environment params",
         test_generic_library.wrangler_assert, {}),
        (lambda_method_function_bricks, "Error validating runtime params",
         test_generic_library.method_assert, {}),
        (lambda_wrangler_function_bricks, "Error validating environment params",
         test_generic_library.wrangler_assert, {})
    ])
def test_value_error(which_lambda, expected_message, assertion,
                     which_environment_variables):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion,
        environment_variables=which_environment_variables)


##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,input_file,prepared_file,which_runtime_variables",
    [
        (lambda_method_function_data, "tests/fixtures/test_ingest_input.json",
         "tests/fixtures/test_method_prepared_output.json",
         method_runtime_variables_data),
        (lambda_method_function_bricks, "tests/fixtures/test_bricks_method_input.json",
         "tests/fixtures/test_bricks_method_prepared_output.json",
         method_runtime_variables_bricks)
    ]
)
def test_method_success(which_lambda, input_file, prepared_file, which_runtime_variables):
    """
    Runs the method function.
    :param None
    :return Test Pass/Fail
    """
    with open(prepared_file, "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open(input_file, "r") as file_2:
        test_data = file_2.read()
    which_runtime_variables["RuntimeVariables"]["data"] = json.loads(test_data)

    output = which_lambda.lambda_handler(
        which_runtime_variables, test_generic_library.context_object)

    produced_data = pd.DataFrame(json.loads(output["data"]))

    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@mock.patch('ingest_takeon_data_wrangler.aws_functions.read_from_s3')
@pytest.mark.parametrize(
    "which_lambda,input_file,wrangler_file,runtime_file," +
    "which_runtime_variables_wrangler,which_runtime_variables_method," +
    "which_environment_variables,which_file_list,wrangler_boto3",
    [
        (lambda_wrangler_function_data, "tests/fixtures/test_ingest_input.json",
         "tests/fixtures/test_wrangler_to_method_input.json",
         "tests/fixtures/test_wrangler_to_method_runtime.json",
         wrangler_runtime_variables_data, method_runtime_variables_data,
         wrangler_environment_variables, ["test_ingest_input.json"],
         "ingest_takeon_data_wrangler.boto3.client"),
        (lambda_wrangler_function_bricks, "tests/fixtures/test_bricks_method_input.json",
         "tests/fixtures/test_bricks_wrangler_to_method_input.json",
         "tests/fixtures/test_bricks_wrangler_to_method_runtime.json",
         wrangler_runtime_variables_bricks, method_runtime_variables_bricks,
         wrangler_environment_variables, ["test_bricks_method_input.json"],
         "ingest_brick_type_wrangler.boto3.client")
    ]
)
def test_wrangler_success_passed(mock_s3_get, which_lambda, input_file, wrangler_file,
                                 runtime_file, which_runtime_variables_wrangler,
                                 which_runtime_variables_method,
                                 which_environment_variables, which_file_list,
                                 wrangler_boto3):
    """
    Runs the wrangler function.
    :param mock_s3_get - Replacement Function For The Data Retrieval AWS Functionality.
    :return Test Pass/Fail
    """
    with open(input_file, "r") as file:
        wrangler_input = json.dumps(file.read())
    mock_s3_get.return_value = wrangler_input
    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = which_file_list

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(which_lambda.os.environ,
                         which_environment_variables):
        with mock.patch(wrangler_boto3) as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            # Rather than mock the get/decode we tell the code that when the invoke is
            # called pass the variables to this replacement function instead.
            mock_client_object.invoke.side_effect =\
                test_generic_library.replacement_invoke

            # This stops the Error caused by the replacement function from stopping
            # the test.
            with pytest.raises(exception_classes.LambdaFailure):
                which_lambda.lambda_handler(
                    which_runtime_variables_wrangler, test_generic_library.context_object
                )

    with open(input_file, "r") as file_2:
        test_data_prepared = file_2.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open(wrangler_file, "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    # Compares the data.
    assert_frame_equal(produced_data, prepared_data)

    with open(runtime_file, "r") as file_4:
        test_dict_prepared = file_4.read()
    produced_dict = json.loads(test_dict_prepared)

    # Ensures data is not in the RuntimeVariables and then compares.
    which_runtime_variables_method["RuntimeVariables"]["data"] = None
    assert produced_dict == which_runtime_variables_method["RuntimeVariables"]


@mock_s3
@mock.patch('ingest_takeon_data_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
@pytest.mark.parametrize(
    "which_lambda,input_file,prepared_method_file,prepared_wrangler_file," +
    "which_environment_variables,which_runtime_variables_wrangler,wrangler_boto3",
    [
        (lambda_wrangler_function_data, "test_ingest_input.json",
         "tests/fixtures/test_method_prepared_output.json",
         "tests/fixtures/test_wrangler_prepared_output.json",
         wrangler_environment_variables, wrangler_runtime_variables_data,
         "ingest_takeon_data_wrangler.boto3.client"),
        (lambda_wrangler_function_bricks, "test_bricks_method_input.json",
         "tests/fixtures/test_bricks_method_prepared_output.json",
         "tests/fixtures/test_bricks_wrangler_prepared_output.json",
         wrangler_environment_variables, wrangler_runtime_variables_bricks,
         "ingest_brick_type_wrangler.boto3.client"),
    ]
)
def test_wrangler_success_returned(mock_s3_put, which_lambda,
                                   input_file, prepared_method_file,
                                   prepared_wrangler_file, which_environment_variables,
                                   which_runtime_variables_wrangler, wrangler_boto3):
    """
    Runs the wrangler function after the method invoke.
    :param None
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = [input_file]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open(prepared_method_file, "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(which_lambda.os.environ,
                         which_environment_variables):
        with mock.patch(wrangler_boto3) as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            mock_client_object.invoke.return_value.get.return_value.read \
                .return_value.decode.return_value = json.dumps({
                 "data": test_data_out,
                 "success": True,
                 "anomalies": []
                })

            output = which_lambda.lambda_handler(
                which_runtime_variables_wrangler, test_generic_library.context_object
            )

    with open(prepared_wrangler_file, "r") as file_3:
        test_data_prepared = file_3.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))
    with open("tests/fixtures/" +
              which_runtime_variables_wrangler["RuntimeVariables"]["out_file_name"],
              "r") as file_4:
        test_data_produced = file_4.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)
