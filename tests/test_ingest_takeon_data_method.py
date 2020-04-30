import json
import unittest.mock as mock

import ingest_takeon_data_method


class MockContext():
    aws_request_id = 666


context_object = MockContext()

payload = {
    "period": "201809",
    "periodicity": "03",
    "RuntimeVariables": {
        "run_id": "o",
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
}


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
            payload["data"] = input_data
            returned_value = ingest_takeon_data_method.lambda_handler(
                payload, context_object
            )

        with open("tests/fixtures/test_results_ingest_output.json") as expected_file:
            expected = expected_file
            assert json.loads(returned_value["data"]) == json.load(expected)

    def test_method_general_exception(self):
        with open("tests/fixtures/takeon-data-export.json") as file:
            input_data = json.load(file)
            with mock.patch(
                "ingest_takeon_data_method.general_functions.calculate_adjacent_periods")\
                    as mocked:
                mocked.side_effect = Exception("General exception")
                payload["data"] = input_data
                response = ingest_takeon_data_method.lambda_handler(
                    payload, context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert "'Exception'" in response["error"]

    def test_method_key_error(self):
        if "data" in payload:
            payload.pop("data")

        returned_value = ingest_takeon_data_method.lambda_handler(
            payload, context_object
        )

        assert "KeyError" in returned_value["error"]
