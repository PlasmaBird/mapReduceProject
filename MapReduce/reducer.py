#!/usr/bin/env python3

from collections import defaultdict
import grpc
from concurrent import futures
import json  # Assuming JSON format for data serialization
import reduce_pb2
import reduce_pb2_grpc
import coordinator_pb2
import coordinator_pb2_grpc
import os
import json

def read_config(config_path='config.json'):
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    return config

COORDINATOR_ADDRESS = "localhost:50070"  # Assuming the coordinator is running on this address


class Reduce(reduce_pb2_grpc.ReduceServiceServicer):

    def __init__(self, coordinator_address):
        # Create a gRPC channel to the coordinator
        self.coordinator_channel = grpc.insecure_channel(coordinator_address)
        # Create a stub to communicate with the coordinator
        self.coordinator_stub = coordinator_pb2_grpc.CoordinatorServiceStub(self.coordinator_channel)


    def Reduce(self, request, context):
        # Assuming the incoming data is serialized as a JSON string in request.data
        # This requires adjusting the ReduceRequest message in your .proto file to include a 'data' field
        try:
            # Deserialize the incoming data
            intermediate_data = json.loads(request.data)

            word_counts = defaultdict(int)
            for word, counts in intermediate_data.items():
                # Aggregate counts for each word
                word_counts[word] = sum(int(count) for count in counts)

            # Generate output data (could be written to a file, returned directly, etc.)
            # Here, we're assuming the reducer sends back a serialized response
            output_data = json.dumps(word_counts)
            # Assuming output data is written to a file, and output_file_path is the path to this file
            output_file_path = "reducer_output.txt"
            with open(output_file_path, 'w') as f:
                f.write(output_data)

            # After writing the output, notify the coordinator of completion
            self.notify_coordinator_completion(output_file_path)

            return reduce_pb2.ReduceResponse(data=output_data, message="Success", status=True)
        except Exception as e:
            return reduce_pb2.ReduceResponse(message=str(e), status=False)

    def notify_coordinator_completion(self, output_file_path):
        # Call the NotifyReducerCompletion method on the coordinator
        response = self.coordinator_stub.NotifyReducerCompletion(
            coordinator_pb2.ReducerCompletionNotification(output_file=output_file_path)
        )
        print(f"Notification sent to Coordinator: {response.message}")

def initialize():
    config = read_config()
    reducer_index = int(os.getenv('REDUCER_INDEX', '0'))  # Default to 0 if not set
    reducer_address = config['reducers'][reducer_index]  # Use the index to get the right address
    coordinator_address = config['coordinator']


    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    reducer_instance = Reduce(coordinator_address)
    reduce_pb2_grpc.add_ReduceServiceServicer_to_server(reducer_instance, server)

    server.add_insecure_port(reducer_address)
    print(f"Starting reducer server on {reducer_address}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    initialize()
