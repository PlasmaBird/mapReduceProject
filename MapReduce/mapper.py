#!/usr/bin/env python3

import re
import os
import mapper_pb2
import mapper_pb2_grpc
import coordinator_pb2
import coordinator_pb2_grpc
import grpc
import threading
from concurrent import futures
import json

def load_config(config_path='config.json'):
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    return config

config = load_config()

COORDINATOR_ADDRESS = "localhost:50070"  # Assuming the coordinator is running on this address

class Mapper(mapper_pb2_grpc.MapperServiceServicer):
    def __init__(self, coordinator_address):
        self.channel = grpc.insecure_channel(coordinator_address)
        self.coordinatorStub = coordinator_pb2_grpc.CoordinatorServiceStub(self.channel)

    def Map(self, request, context):
        # Open the specified input file
        try:
            input_file = request.input_file
            # Generate a unique output filename for intermediate data
            output_file = f"{os.path.splitext(input_file)[0]}_mapped.txt"

            with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
                # Read each line in the file
                for line in infile:
                    # Normalize case to make counting case-insensitive
                    line = line.lower()
                    # Split the line into words using regular expression to match words
                    words = re.findall(r'\w+', line)
                    # Output each word with a count of 1 to the output file
                    for word in words:
                        outfile.write(f"{word}\t1\n")

            # After mapping is done, notify the coordinator with the location of the intermediate data
            self.notify_coordinator(input_file, output_file)

            return mapper_pb2.MapResponse(message="Success", status=True)
        except Exception as e:
            return mapper_pb2.MapResponse(message=str(e), status=False)
        
    def notify_coordinator(self, input_file, output_file):
        try:
            response = self.coordinatorStub.NotifyMapperCompletion(
                coordinator_pb2.MapperCompletionNotification(
                    input_file=input_file,
                    output_file=output_file
                )
            )
            print(f"Notification sent to Coordinator: {response.message}")
        except grpc.RpcError as e:
            print(f"Failed to notify coordinator: {str(e)}")


def initialize():
    config = load_config()
    coordinator_address = config['coordinator']
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    mapper_instance = Mapper(coordinator_address)
    mapper_pb2_grpc.add_MapperServiceServicer_to_server(mapper_instance, server)
    
    # Determine the mapper's own address from the config and use it to start the server
    # This assumes the mapper knows its index or identifier; here we're just using the first one for simplicity
    mapper_address = config['mappers'][0]
    server.add_insecure_port(mapper_address)
    
    print(f"Starting mapper server on {mapper_address}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    initialize()
