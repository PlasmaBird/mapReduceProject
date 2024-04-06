#!/usr/bin/env python3
from pymongo.mongo_client import MongoClient
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
import time
import threading

# MongoDB connection string
MONGO_URI = "mongodb+srv://admin4459:admin4459@cluster0.qpwu0ez.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = None
db = None
collection = None
initialization_lock = threading.Lock()


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
            if insertFileInMongoDB(output_file):
                return reduce_pb2.ReduceResponse(message="Success", status=True)
            else:
                return reduce_pb2.ReduceResponse(message="Failed to store the file in MongoDB", status=False)
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


def initialize_db():
    with initialization_lock:
        global client, db, collection
        try:
            print("in reducer initialize mongoDB")
            client = MongoClient(MONGO_URI)
            time.sleep(2)
            db = client['4459']
            collection = db['OUTPUT_FILES']
            client.admin.command('ping')
            print("Successfully connected to MongoDB.")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}. Please restart the server.")

def initialize_server():
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


def insertFileInMongoDB(file_to_store):
    try:
        with open(file_to_store, 'r') as file:
            file_content = file.read()

        # Find the highest counter value in the collection
        last_document = collection.find_one(sort=[("file_id", -1)]) 
        time.sleep(1)
        # Sort documents by counter in descending order
        # If the collection is empty, start the counter at 1; otherwise, increment the counter by 1
        new_counter = 1 if last_document is None else last_document['file_id'] + 1
        time.sleep(1)
        # Storing the file
        collection.insert_one({'file_id': new_counter, 'filename': file_to_store, 'file_data': file_content})
        time.sleep(1)
        print(f"{file_to_store} with file_id {new_counter} inserted successfully")
        return True
    except Exception as e:
        print(str(e))
        return False
    

if __name__ == "__main__":
    initialize_db()
    time.sleep(2)
    initialize_server()
