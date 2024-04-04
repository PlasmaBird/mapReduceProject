import unittest
from unittest.mock import MagicMock, patch
from mapper import initialize

class TestInitialize(unittest.TestCase):
    @patch('mapper.grpc.server')
    @patch('mapper.Mapper')
    @patch('mapper.mapper_pb2_grpc.add_MapperServiceServicer_to_server')
    @patch('mapper.heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server')
    @patch('mapper.threading.Thread')
    def test_initialize(self, mock_thread, mock_add_heartbeat, mock_add_mapper, mock_mapper, mock_server):
        # Mock objects
        mock_server_instance = MagicMock()
        mock_mapper_instance = MagicMock()
        mock_heartbeat_instance = MagicMock()
        mock_thread_instance = MagicMock()

        # Configure mock objects
        mock_server.return_value = mock_server_instance
        mock_mapper.return_value = mock_mapper_instance
        mock_thread.return_value = mock_thread_instance

        # Call the function
        initialize()

        # Assert that the server was started
        mock_server_instance.start.assert_called_once()

        # Assert that the heartbeat thread was started
        mock_thread_instance.start.assert_called_once()

        # Assert that the server was waiting for termination
        mock_server_instance.wait_for_termination.assert_called_once()

if __name__ == '__main__':
    unittest.main()