import json
import unittest
import unittest.mock as mock

from botocore.response import StreamingBody
from es_aws_functions import exception_classes

import ingest_takeon_data_wrangler


class MockContext:
    aws_request_id = 666


context_object = MockContext()

runtime_variables = {'RuntimeVariables': {"run_id": "bob", "queue_url": "Earl"}}


class TestIngestTakeOnData():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "takeon_bucket_name": "mock-bucket",
                "results_bucket_name": "mock-bucket",
                "in_file_name": "mock-file",
                "method_name": "mock-function",
                "period": "201809",
                "checkpoint": "0",
                "sqs_queue_url": "mock-queue-url",
                "sqs_message_group_id": "mock-messageid",
                "sns_topic_arn": "mock-topic-arn",
                "out_file_name": "outie"
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

            assert "General Error" in exc_info.exception.error_message

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
                    "sqs_message_group_id": "mock-group-id",
                },
        ):
            # Removing the method_name to allow for test of missing parameter
            ingest_takeon_data_wrangler.os.environ.pop("method_name")
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                ingest_takeon_data_wrangler.lambda_handler(
                    {"RuntimeVariables":
                        {"checkpoint": 123, "period": "201809", "run_id": "bob",
                         "queue_url": "Earl"}},
                    context_object,
                )
            assert "Blank or empty environment variable in" in \
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
            assert "Incomplete Lambda response" in exc_info.exception.error_message

    @mock.patch('ingest_takeon_data_wrangler.aws_functions.send_sns_message')
    @mock.patch('ingest_takeon_data_wrangler.boto3.client')
    def test_aws_error(self, mock_client, mock_sns):
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            ingest_takeon_data_wrangler.lambda_handler(
                runtime_variables, context_object
            )
        assert "AWS Error" in exc_info.exception.error_message

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
            assert "Key Error" in exc_info.exception.error_message
