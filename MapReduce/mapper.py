#!/usr/bin/env python3
import time
from pymongo.mongo_client import MongoClient
import re
import mapper_pb2
import mapper_pb2_grpc
import reduce_pb2
import reduce_pb2_grpc
import heartbeat_pb2
import heartbeat_pb2_grpc
import grpc
import time
import threading
from concurrent import futures


REDUCER_ADDRESS = "localhost:50052"
MONGO_URI = "mongodb+srv://admin4459:admin4459@cluster0.qpwu0ez.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = None
db = None
collection = None
initialization_lock = threading.Lock()

class Heartbeat(heartbeat_pb2_grpc.HeartbeatServiceServicer):
    def SendHeartbeat(self, request, context):
        print(f"Heartbeat received from {request.sender}")
        return heartbeat_pb2.HeartbeatResponse(acknowledged=True)

    
class Mapper(mapper_pb2_grpc.MapperServiceServicer):
    def __init__(self):
        self.channel = grpc.insecure_channel(REDUCER_ADDRESS)
        self.reduceStub = reduce_pb2_grpc.ReduceServiceStub(self.channel)
        self.heartbeatStub = heartbeat_pb2_grpc.HeartbeatServiceStub(self.channel)

    def Map(self, request, context):
        # Open the specified input file
        try:
            input_file = request.input_file
            output_file = request.output_file
            tempOutputFile = "mapped_file_output.txt"
            with open(input_file, 'r') as infile, open(tempOutputFile, 'w') as outfile:
                # Read each line in the file
                for line in infile:
                    # Normalize case to make counting case-insensitive
                    line = line.lower()
                    # Split the line into words using regular expression to match words
                    words = re.findall(r'\w+', line)
                    # Output each word with a count of 1 to the output file
                    for word in words:
                        outfile.write(f"{word}\t1\n")
            
            # Call the reducer to reduce the mapped output and check for any errors
            reducerResponse = self.reduceStub.Reduce(reduce_pb2.ReduceRequest(input_file=tempOutputFile, output_file=output_file))
            if reducerResponse.status:
                if insertFileInMongoDB(input_file):
                    return mapper_pb2.MapResponse(message="Success", status=True)
                else:
                    return mapper_pb2.MapResponse(message="Failed to store the file in MongoDB", status=False)
            else:
                return mapper_pb2.MapResponse(message=reducerResponse.message, status=False)
        except Exception as e:
            return mapper_pb2.MapResponse(message=str(e), status=False)
        
    def send_heartbeat_periodically(self):
        while True:
            try:
                response = self.heartbeatStub.SendHeartbeat(heartbeat_pb2.HeartbeatRequest(sender="Mapper"))
                if response.acknowledged:
                    print("Heartbeat acknowledged by Reducer")
                else:
                    print("Heartbeat not acknowledged")
            except Exception as e:
                print(f"Heartbeat failed: {e}")
            time.sleep(5)  # Send a heartbeat every 5 seconds


def initialize_db():
    with initialization_lock:
        global client, db, collection
        try:
            print("in mapper initialize mongoDB")
            client = MongoClient(MONGO_URI)
            time.sleep(2)
            db = client['4459']
            collection = db['INPUT_FILES']
            client.admin.command('ping')

            print("Successfully connected to MongoDB.")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}. Please restart the server.")

def initialize_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    mapper_instance = Mapper()  # Create an instance of the Mapper class
    mapper_pb2_grpc.add_MapperServiceServicer_to_server(mapper_instance, server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(Heartbeat(), server)
    
    server.add_insecure_port('localhost:50051')
    print("starting mapper server...")
    server.start()

    # Start the heartbeat thread
    heartbeat_thread = threading.Thread(target=mapper_instance.send_heartbeat_periodically)
    heartbeat_thread.daemon = True  # This ensures the thread doesn't prevent the program from exiting
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
