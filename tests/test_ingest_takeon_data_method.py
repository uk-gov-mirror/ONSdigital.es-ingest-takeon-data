import json
import unittest.mock as mock

import ingest_takeon_data_method


class MockContext():
    aws_request_id = 666


context_object = MockContext()


class TestIngestTakeOnData():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "period": "201809"
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    def test_method_happy_path(self):
        with open("tests/fixtures/takeon-data-export.json") as input_file:
            input_data = json.load(input_file)
            returned_value = ingest_takeon_data_method.lambda_handler(
                input_data, None
            )

        with open("tests/fixtures/test_results_ingest_output.json") as expected_file:
            expected = expected_file

            assert returned_value == json.load(expected)

    def test_method_general_exception(self):
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            with mock.patch("ingest_takeon_data_method.InputSchema.load") as mocked:
                mocked.side_effect = Exception("General exception")
                response = ingest_takeon_data_method.lambda_handler(
                    input_data, context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    def test_method_key_error(self):
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            ingest_takeon_data_method.os.environ.pop("period")
            returned_value = ingest_takeon_data_method.lambda_handler(
                json.dumps(input_data), context_object
            )
            ingest_takeon_data_method.os.environ["period"] = "201809"

            assert """Key Error""" in returned_value["error"]
