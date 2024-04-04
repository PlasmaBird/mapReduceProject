import unittest
from unittest.mock import MagicMock
from mapper import Mapper
import reduce_pb2

class TestMapper(unittest.TestCase):
    def setUp(self):
        self.mapper = Mapper()
        self.mapper.reduceStub = MagicMock()

    def test_map_success(self):
        # Arrange
        request = MagicMock()
        request.input_file = "input_file.txt"
        request.output_file = "output_file.txt"

        # Act
        response = self.mapper.Map(request, None)

        # Assert
        self.assertTrue(response.status)
        self.assertEqual(response.message, "Success")
        self.mapper.reduceStub.Reduce.assert_called_once_with(reduce_pb2.ReduceRequest(input_file="mapped_file_output.txt", output_file="output_file.txt"))

    def test_map_failure(self):
        # Arrange
        request = MagicMock()
        request.input_file = "input_file.txt"
        request.output_file = "output_file.txt"
        self.mapper.reduceStub.Reduce.return_value = reduce_pb2.ReduceResponse(status=False, message="Reducer error")

        # Act
        response = self.mapper.Map(request, None)

        # Assert
        self.assertFalse(response.status)
        self.assertEqual(response.message, "Reducer error")
        self.mapper.reduceStub.Reduce.assert_called_once_with(reduce_pb2.ReduceRequest(input_file="mapped_file_output.txt", output_file="output_file.txt"))

    def test_map_exception(self):
        # Arrange
        request = MagicMock()
        request.input_file = "input_file.txt"
        request.output_file = "output_file.txt"
        self.mapper.reduceStub.Reduce.side_effect = Exception("Reducer exception")

        # Act
        response = self.mapper.Map(request, None)

        # Assert
        self.assertFalse(response.status)
        self.assertEqual(response.message, "Reducer exception")

if __name__ == '__main__':
    unittest.main()