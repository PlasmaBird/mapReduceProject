#!/usr/bin/env python3

from collections import defaultdict
import grpc
from concurrent import futures
import reduce_pb2
import reduce_pb2_grpc
import heartbeat_pb2
import heartbeat_pb2_grpc
import threading
import time
import os

MAPPER_ADDRESS = "localhost:50051"


class Heartbeat(heartbeat_pb2_grpc.HeartbeatServiceServicer):
    def SendHeartbeat(self, request, context):
        print(f"Heartbeat received from {request.sender}")
        return heartbeat_pb2.HeartbeatResponse(acknowledged=True)
    
class Reduce(reduce_pb2_grpc.ReduceServiceServicer):
    def __init__(self):
        self.mapper_heartbeat_channel = grpc.insecure_channel(MAPPER_ADDRESS)
        self.mapper_heartbeat_stub = heartbeat_pb2_grpc.HeartbeatServiceStub(self.mapper_heartbeat_channel)

    def Reduce(self, request, context):
        input_file = request.input_file
        output_file = request.output_file

        word_counts = defaultdict(int)
        try:
            # Read the output file from the mapper
            with open(input_file, 'r') as infile:
                for line in infile:
                    word, count = line.strip().split('\t')
                    word_counts[word] += int(count)
            # The dictionary automatically handles sorting by key (word) and reduction
            # Writing the reduced word counts to a new output file
            with open(output_file, 'w') as outfile:
                for word, count in sorted(word_counts.items()):
                    outfile.write(f"{word}\t{count}\n")
            os.remove(input_file)
            return reduce_pb2.ReduceResponse(message="Success", status=True)
        except FileNotFoundError:
            pass
        except Exception as e:
            return reduce_pb2.ReduceResponse(message=str(e), status=False)
        
    def send_heartbeat_periodically(self):
        while True:
            try:
                response = self.mapper_heartbeat_stub.SendHeartbeat(heartbeat_pb2.HeartbeatRequest(sender="Reducer"))
                if response.acknowledged:
                    print("Heartbeat acknowledged by Mapper")
                else:
                    print("Heartbeat not acknowledged by Mapper")
            except Exception as e:
                print(f"Heartbeat sending failed: {e}")
            time.sleep(5)  # Adjust the sleep time as needed

def initialize():
    #INFO - can get comment out the next 2 lines if you don't want to delete the file
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    reducer_instance = Reduce()  # Create an instance of the Reduce class
    reduce_pb2_grpc.add_ReduceServiceServicer_to_server(reducer_instance, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(Heartbeat(), server)

    server.add_insecure_port('localhost:50052')
    print("starting reducer server...")
    server.start()

    # Start the heartbeat thread
    heartbeat_thread = threading.Thread(target=reducer_instance.send_heartbeat_periodically)
    heartbeat_thread.daemon = True  # Allows the program to exit even if the thread is running
    heartbeat_thread.start()

    server.wait_for_termination()
    exit()


if __name__ == "__main__":
    # Expect the input and output file name as a command-line argument
    # reducer()
    initialize()
