import json
import unittest
import unittest.mock as mock

from botocore.response import StreamingBody
from es_aws_functions import exception_classes

import ingest_takeon_data_wrangler


class MockContext:
    aws_request_id = 666


context_object = MockContext()

runtime_variables = {'RuntimeVariables': {
    "run_id": "bob",
    "queue_url": "Earl",
    "in_file_name": "mock-file",
    "out_file_name": "outie",
    "outgoing_message_group_id": "mock_out_group",
    "period": "202020",
    "periodicity": "03",
    "sns_topic_arn": "mock-topic-arn",
    "location": "Here",
    "ingestion_parameters": {
        "question_labels": {
            '0601': 'Q601_asphalting_sand',
            '0602': 'Q602_building_soft_sand',
            '0603': 'Q603_concreting_sand',
            '0604': 'Q604_bituminous_gravel',
            '0605': 'Q605_concreting_gravel',
            '0606': 'Q606_other_gravel',
            '0607': 'Q607_constructional_fill',
            '0608': 'Q608_total'
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


class TestIngestTakeOnData():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "takeon_bucket_name": "mock-bucket",
                "results_bucket_name": "mock-bucket",
                "method_name": "mock-function",
                "checkpoint": "0",
                "sqs_queue_url": "mock-queue-url"
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock.patch("ingest_takeon_data_wrangler.boto3.client")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.read_from_s3")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.save_data")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.send_sns_message")
    def test_happy_path(self, mock_sns_return, mock_s3_write, mock_s3_return, mock_client):  # noqa: E501
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)
            with open("tests/fixtures/test_results_ingest_output.json", "r") as file:
                mock_client_object.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = \
                    json.dumps({"data": json.load(file), "success": True})
                returned_value = ingest_takeon_data_wrangler.lambda_handler(
                    runtime_variables, context_object
                )

                assert "success" in returned_value
                assert returned_value["success"] is True

    @mock.patch("ingest_takeon_data_wrangler.boto3.client")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.read_from_s3")
    def test_general_exception(self, mock_s3_return, mock_client):
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)
            mock_s3_return.side_effect = Exception("General exception")
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                ingest_takeon_data_wrangler.lambda_handler(
                    runtime_variables, context_object
                )

            assert "'Exception'" in exc_info.exception.error_message

    def test_missing_env_var(self):

        """
        Testing the marshmallow in the wrangler raises an exception.
        :return: None.
        """
        with mock.patch.dict(
                ingest_takeon_data_wrangler.os.environ,
                {
                    "arn": "mock:arn",
                    "checkpoint": "mock-checkpoint",
                    "method_name": "mock-name",
                },
        ):
            # Removing the method_name to allow for test of missing parameter
            ingest_takeon_data_wrangler.os.environ.pop("method_name")
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                ingest_takeon_data_wrangler.lambda_handler(
                    {"RuntimeVariables":
                        {"checkpoint": 123, "period": "201809", "run_id": "bob",
                         "queue_url": "Earl", "run_id": "bob"}},
                    context_object,
                )
            assert "Error validating environment param" in \
                   exc_info.exception.error_message

    @mock.patch('ingest_takeon_data_wrangler.aws_functions.send_sns_message')
    @mock.patch('ingest_takeon_data_wrangler.boto3.client')
    @mock.patch('ingest_takeon_data_wrangler.aws_functions.read_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_client, mock_sns):
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)

        mock_get_from_s3.return_value = json.dumps(input_data)

        with open('tests/fixtures/test_results_ingest_output.json', "rb") as file:
            mock_client.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 2)}

            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                ingest_takeon_data_wrangler.lambda_handler(
                    runtime_variables, context_object
                )
            assert "IncompleteReadError" in exc_info.exception.error_message

    @mock.patch('ingest_takeon_data_wrangler.aws_functions.send_sns_message')
    @mock.patch('ingest_takeon_data_wrangler.boto3.client')
    def test_aws_error(self, mock_client, mock_sns):
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            ingest_takeon_data_wrangler.lambda_handler(
                runtime_variables, context_object
            )
        assert "ClientError" in exc_info.exception.error_message

    @mock.patch("ingest_takeon_data_wrangler.boto3.client")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.read_from_s3")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.save_data")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.send_sns_message")
    def test_method_fail(self, mock_sns_return, mock_s3_write,
                         mock_s3_return, mock_client):
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)

            mock_client_object.invoke.return_value.get.return_value \
                .read.return_value.decode.return_value = \
                json.dumps({"error": "This is an error message", "success": False})

            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                ingest_takeon_data_wrangler.lambda_handler(
                    runtime_variables, context_object
                )
            assert "error message" in exc_info.exception.error_message

    @mock.patch("ingest_takeon_data_wrangler.boto3.client")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.read_from_s3")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.save_data")
    @mock.patch("ingest_takeon_data_wrangler.aws_functions.send_sns_message")
    def test_key_error(self, mock_sns_return, mock_s3_write,
                       mock_s3_return, mock_client):
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)

            mock_client_object.invoke.return_value.get.return_value \
                .read.return_value.decode.return_value = \
                json.dumps({"error": "This is an error message", "NotRight": False})

            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                ingest_takeon_data_wrangler.lambda_handler(
                    runtime_variables, context_object
                )
            assert "KeyError" in exc_info.exception.error_message
