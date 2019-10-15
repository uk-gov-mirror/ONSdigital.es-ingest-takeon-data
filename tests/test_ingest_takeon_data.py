import json
import unittest.mock as mock

import boto3
from botocore.response import StreamingBody
from moto import mock_s3, mock_sns

import ingest_takeon_data


class TestIngestTakeOnData():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "takeon_bucket_name": "mock-bucket",
                "results_bucket_name": "mock-bucket",
                "file_name": "mock-file",
                "function_name": "mock-function",
                "period": "201809",
                "checkpoint": "0",
                "sqs_queue_url": "mock-queue-url",
                "sqs_messageid_name": "mock-messageid",
                "sns_topic_arn": "mock-topic-arn"
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock.patch("ingest_takeon_data.boto3.client")
    @mock.patch("ingest_takeon_data.read_from_s3")
    @mock.patch("ingest_takeon_data.write_to_s3")
    @mock.patch("ingest_takeon_data.send_sns_message")
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

                returned_value = ingest_takeon_data.lambda_handler(
                    None, None
                )

                assert "success" in returned_value
                assert returned_value["success"] is True

    @mock.patch("ingest_takeon_data.boto3.client")
    @mock.patch("ingest_takeon_data.read_from_s3")
    def test_general_exception(self, mock_s3_return, mock_client):
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            mock_s3_return.return_value = json.dumps(input_data)
            mock_s3_return.side_effect = Exception("General exception")

            returned_value = ingest_takeon_data.lambda_handler(
                None, None
            )

            assert "success" in returned_value
            assert returned_value["success"] is False
            assert """General exception""" in returned_value["error"]

    def test_missing_env_var(self):
        ingest_takeon_data.os.environ.pop("file_name")
        returned_value = ingest_takeon_data.lambda_handler(
            None, None
        )
        ingest_takeon_data.os.environ["file_name"] = "mock-file"

        assert """Key Error""" in returned_value["error"]

    def test_empty_env_var(self):
        ingest_takeon_data.os.environ["file_name"] = ""
        returned_value = ingest_takeon_data.lambda_handler(
            None, None
        )
        ingest_takeon_data.os.environ["file_name"] = "mock-file"

        assert """Blank or empty environment variable in """ in returned_value["error"]

    @mock_sns
    def test_wrangler_fail_to_send_to_sns(self):
        with mock.patch.dict(
            ingest_takeon_data.os.environ,
            {
                "sns_topic_arn": "An Invalid Arn"
            },
        ):
            response = ingest_takeon_data.lambda_handler(
                None, None
            )
            assert "success" in response
            assert response["success"] is False
            assert """AWS Error""" in response["error"]

    @mock.patch('ingest_takeon_data.send_sns_message')
    @mock.patch('ingest_takeon_data.boto3.client')
    @mock.patch('ingest_takeon_data.read_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_client, mock_sns):
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)

        mock_get_from_s3.return_value = json.dumps(input_data)

        with open('tests/fixtures/test_results_ingest_output.json', "rb") as file:
            mock_client.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 2)}

            returned_value = ingest_takeon_data.lambda_handler(
                None, None
            )

        assert(returned_value['error'].__contains__("""Incomplete Lambda response"""))

    @mock_s3
    def test_get_and_save_data_from_to_s3(self, s3):
        client = boto3.client(
            "s3",
            region_name="eu-west-2",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="TEMP")

        with open("tests/fixtures/takeon-data-export.json", "rb") as file:

            ingest_takeon_data.write_to_s3("TEMP", "123", json.load(file))

            response = ingest_takeon_data.read_from_s3("TEMP", "123")

            with open("tests/fixtures/takeon-data-export.json", "rb") as compare_file:

                assert json.loads(response) == json.load(compare_file)

    @mock_sns
    def test_publish_sns(self, sns):
        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        out = ingest_takeon_data.send_sns_message("3", topic_arn)

        assert (out['ResponseMetadata']['HTTPStatusCode'] == 200)
