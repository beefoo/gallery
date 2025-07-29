import unittest
from unittest.mock import patch, Mock
import locgov_data as ld


class TestMakeRequest(unittest.TestCase):
    """
    This class holds a collection of unit tests designed to confirm that
    locgov_data's make_request() function is operating as intended.
    """

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_successful_request(self, mock_get):
        # Mock a successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_get.return_value = mock_response
        result = ld.make_request("http://example.com", json=True, max_attempts=4)
        self.assertEqual(result, (False, {"data": "test"}))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_not_found(self, mock_get):
        mock_get.return_value = Mock(status_code=404)
        result = ld.make_request("http://example.com/not_found", max_attempts=4)
        self.assertEqual(result, (False, "ERROR - NO RECORD"))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_forbidden(self, mock_get):
        mock_get.return_value = Mock(status_code=403)
        result = ld.make_request("http://example.com/forbidden", max_attempts=4)
        self.assertEqual(result, (False, "ERROR - GENERAL"))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_server_error(self, mock_get):
        mock_get.return_value = Mock(status_code=500)
        result = ld.make_request("http://example.com/server_error", max_attempts=4)
        self.assertEqual(result, (False, "ERROR - GENERAL"))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_too_many_requests(self, mock_get):
        mock_get.return_value = Mock(status_code=429)
        result = ld.make_request("http://example.com/too_many_requests", max_attempts=4)
        self.assertEqual(result, (True, "ERROR - BLOCKED"))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_invalid_json(self, mock_get):
        mock_get.return_value = Mock(status_code=200, json=lambda: b'{"invalid": "dat')
        result = ld.make_request(
            "http://example.com/invalid_json", locgov_json=True, max_attempts=4
        )
        self.assertEqual(result, (False, "ERROR - INVALID JSON"))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_invalid_json_raises(self, mock_get):
        mock_get.return_value = Mock(status_code=200)
        mock_get.return_value.json.side_effect = ValueError("Invalid JSON")
        result = ld.make_request(
            "http://example.com/invalid_json_exception",
            locgov_json=True,
            max_attempts=4,
        )
        self.assertEqual(result, (False, "ERROR - INVALID JSON"))

    @patch("locgov_data.helpers.general.requests.Session.get")
    def test_retry_on_server_error(self, mock_get):
        mock_get.side_effect = [
            Mock(status_code=500),  # First attempt fails
            Mock(
                status_code=200, json=lambda: {"data": "test"}
            ),  # Second attempt succeeds
        ]
        result = ld.make_request(
            "http://example.com/retry_500_then_200", json=True, max_attempts=4
        )
        self.assertEqual(result, (False, {"data": "test"}))
        self.assertEqual(mock_get.call_count, 2)  # Ensure it retried once


def main():
    unittest.main()


if __name__ == "__main__":
    main()
