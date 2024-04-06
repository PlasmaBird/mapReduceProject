import unittest
from unittest.mock import MagicMock, patch
from mapper import Mapper

class TestMapper(unittest.TestCase):
    def setUp(self):
        self.mapper = Mapper()

    @patch('time.sleep', MagicMock())  # Mocking time.sleep to avoid actual sleep during tests
    def test_send_heartbeat_periodically_acknowledged(self):
        self.mapper.heartbeatStub.SendHeartbeat.return_value = MagicMock(acknowledged=True)
        with patch('builtins.print') as mock_print:
            self.mapper.send_heartbeat_periodically()
            mock_print.assert_called_with("Heartbeat acknowledged by Reducer")

    @patch('time.sleep', MagicMock())  # Mocking time.sleep to avoid actual sleep during tests
    def test_send_heartbeat_periodically_not_acknowledged(self):
        self.mapper.heartbeatStub.SendHeartbeat.return_value = MagicMock(acknowledged=False)
        with patch('builtins.print') as mock_print:
            self.mapper.send_heartbeat_periodically()
            mock_print.assert_called_with("Heartbeat not acknowledged")

    @patch('time.sleep', MagicMock())  # Mocking time.sleep to avoid actual sleep during tests
    def test_send_heartbeat_periodically_exception(self):
        self.mapper.heartbeatStub.SendHeartbeat.side_effect = Exception("Some error")
        with patch('builtins.print') as mock_print:
            self.mapper.send_heartbeat_periodically()
            mock_print.assert_called_with("Heartbeat failed: Some error")

if __name__ == '__main__':
    unittest.main()