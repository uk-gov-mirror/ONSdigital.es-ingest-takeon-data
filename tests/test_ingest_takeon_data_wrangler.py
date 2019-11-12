import json
import unittest.mock as mock

from botocore.response import StreamingBody

import ingest_takeon_data_wrangler


class MockContext():
    aws_request_id = 666


context_object = MockContext()


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
    @mock.patch("ingest_takeon_data_wrangler.funk.read_from_s3")
    @mock.patch("ingest_takeon_data_wrangler.funk.save_data")
    @mock.patch("ingest_takeon_data_wrangler.funk.send_sns_message")
    def test_happy_path(self, mock_sns_return, mock_s3_write, mock_s3_return, mock_client):  # noqa: E501
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)
            with open("tests/fixtures/takeon-data-export.json", "rb") as file:
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody(file, 1261217)
                }

                returned_value = ingest_takeon_data_wrangler.lambda_handler(
                    None, context_object
                )

                assert "success" in returned_value
                assert returned_value["success"] is True

    @mock.patch("ingest_takeon_data_wrangler.boto3.client")
    @mock.patch("ingest_takeon_data_wrangler.funk.read_from_s3")
    def test_general_exception(self, mock_s3_return, mock_client):
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)
            mock_s3_return.side_effect = Exception("General exception")

            returned_value = ingest_takeon_data_wrangler.lambda_handler(
                None, context_object
            )

            assert "success" in returned_value
            assert returned_value["success"] is False
            assert """General exception""" in returned_value["error"]

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
                    "sqs_queue_url": "sqs_queue_url",
                },
        ):
            # Removing the method_name to allow for test of missing parameter
            ingest_takeon_data_wrangler.os.environ.pop("method_name")
            response = ingest_takeon_data_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 123, "period": "201809"}},
                context_object,
            )

            assert response["error"].__contains__(
                """Error validating environment parameters:"""
            )

    def test_empty_env_var(self):
        ingest_takeon_data_wrangler.os.environ["in_file_name"] = ""
        returned_value = ingest_takeon_data_wrangler.lambda_handler(
            None, context_object
        )
        ingest_takeon_data_wrangler.os.environ["in_file_name"] = "mock-file"

        assert """Blank or empty environment variable in """ in returned_value["error"]

    @mock.patch('ingest_takeon_data_wrangler.funk.send_sns_message')
    @mock.patch('ingest_takeon_data_wrangler.boto3.client')
    @mock.patch('ingest_takeon_data_wrangler.funk.read_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_client, mock_sns):
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)

        mock_get_from_s3.return_value = json.dumps(input_data)

        with open('tests/fixtures/test_results_ingest_output.json', "rb") as file:
            mock_client.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 2)}

            returned_value = ingest_takeon_data_wrangler.lambda_handler(
                None, context_object
            )

        assert(returned_value['error'].__contains__("""Incomplete Lambda response"""))

    @mock.patch('ingest_takeon_data_wrangler.funk.send_sns_message')
    @mock.patch('ingest_takeon_data_wrangler.boto3.client')
    def test_aws_error(self, mock_client, mock_sns):

        returned_value = ingest_takeon_data_wrangler.lambda_handler(
            None, context_object
        )

        assert("AWS Error" in returned_value['error'])
