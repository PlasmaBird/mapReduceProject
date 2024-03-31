#!/usr/bin/env python3

from collections import defaultdict
import grpc
from concurrent import futures
import reduce_pb2
import reduce_pb2_grpc
import os

class Reduce(reduce_pb2_grpc.ReduceServiceServicer):
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

def initialize():
    #INFO - can get comment out the next 2 lines if you don't want to delete the file
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    reduce_pb2_grpc.add_ReduceServiceServicer_to_server(Reduce(), server)
    server.add_insecure_port('localhost:50052')
    print("starting reducer server...")
    server.start()
    server.wait_for_termination()
    exit()
    # Add an indented block of code here


if __name__ == "__main__":
     # Expect the input and output file name as a command-line argumen
    # reducer()
    initialize()
