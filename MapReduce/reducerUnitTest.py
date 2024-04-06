import unittest
from collections import defaultdict
from reducer import Reduce
import os

class ReduceTestCase(unittest.TestCase):
    def test_reduce_success(self):
        # Create a mock request object
        class MockRequest:
            def __init__(self, input_file, output_file):
                self.input_file = input_file
                self.output_file = output_file

        # Create a mock context object
        class MockContext:
            pass

        # Create an instance of the Reduce class
        reducer = Reduce()

        # Set up the input and output file paths
        input_file = "input.txt"
        output_file = "output.txt"

        # Create a mock input file with some word counts
        with open(input_file, 'w') as infile:
            infile.write("apple\t3\n")
            infile.write("banana\t2\n")
            infile.write("orange\t1\n")

        # Call the Reduce method with the mock request and context
        response = reducer.Reduce(MockRequest(input_file, output_file), MockContext())

        # Assert that the response message is "Success"
        self.assertEqual(response.message, "Success")

        # Assert that the output file was created and contains the correct word counts
        with open(output_file, 'r') as outfile:
            lines = outfile.readlines()
            self.assertEqual(len(lines), 3)
            self.assertEqual(lines[0], "apple\t3\n")
            self.assertEqual(lines[1], "banana\t2\n")
            self.assertEqual(lines[2], "orange\t1\n")

        # Clean up the input and output files
        os.remove(input_file)
        os.remove(output_file)

    def test_reduce_file_not_found(self):
        # Create a mock request object
        class MockRequest:
            def __init__(self, input_file, output_file):
                self.input_file = input_file
                self.output_file = output_file

        # Create a mock context object
        class MockContext:
            pass

        # Create an instance of the Reduce class
        reducer = Reduce()

        # Set up the input and output file paths
        input_file = "nonexistent_input.txt"
        output_file = "output.txt"

        # Call the Reduce method with the mock request and context
        response = reducer.Reduce(MockRequest(input_file, output_file), MockContext())

        # Assert that the response status is False
        self.assertFalse(response.status)

        # Assert that the response message is the expected error message
        self.assertEqual(response.message, "[Errno 2] No such file or directory: 'nonexistent_input.txt'")

    # Add more test cases as needed

if __name__ == "__main__":
    unittest.main()