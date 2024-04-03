import json
import threading
from concurrent import futures
import grpc
import mapper_pb2
import mapper_pb2_grpc
import reduce_pb2
import reduce_pb2_grpc
import coordinator_pb2
import coordinator_pb2_grpc
import random
counter = 0


class CoordinatorService(coordinator_pb2_grpc.CoordinatorServiceServicer):
    def __init__(self):
        # Load mapper addresses from configuration and create stubs
        self.read_config()
        self.mapper_stubs = self.create_mapper_stubs()
        self.reducer_stubs = self.create_reducer_stubs()
        self.mapper_outputs = []  # Store mapper output file locations
    
    def read_config(self,config_path='config.json'):
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)

    def create_mapper_stubs(self):
        # Similar to the create_reducer_stubs method, but for mappers
        stubs = []
        for address in self.config['mappers']:
            channel = grpc.insecure_channel(address)
            stub = mapper_pb2_grpc.MapperServiceStub(channel)
            stubs.append(stub)
        return stubs
        
    def create_reducer_stubs(self):
        # Initialize gRPC stubs for all reducers
        stubs = []
        for address in self.config['reducers']:
            # Create a gRPC channel for each reducer address
            channel = grpc.insecure_channel(address)
            # Initialize the stub using the channel. You should use the appropriate stub class for the reducer service.
            stub = reduce_pb2_grpc.ReduceServiceStub(channel)
            stubs.append(stub)
        return stubs


    def StartJob(self, request, context):
        input_location = request.input_location
        input_splits = self.split_input_data(input_location)
        self.distribute_tasks_to_mappers(input_splits)

        return coordinator_pb2.JobResponse(message="Job completed", status=True, job_id="your_job_id")
    
    def split_input_data(self, input_location, lines_per_chunk=100):
        chunks = []
        try:
            with open(input_location, 'r') as infile:
                lines = infile.readlines()
            
            # Calculate the number of chunks needed
            total_lines = len(lines)
            num_chunks = (total_lines + lines_per_chunk - 1) // lines_per_chunk  # Ceiling division

            # Split lines into chunks and write each to a new file
            for i in range(num_chunks):
                chunk_lines = lines[i*lines_per_chunk : (i+1)*lines_per_chunk]
                chunk_file = f"{input_location}_chunk_{i}.txt"
                with open(chunk_file, 'w') as outfile:
                    outfile.writelines(chunk_lines)
                chunks.append(chunk_file)

        except IOError as e:
            print(f"Error reading or writing input data: {e}")

        return chunks


    def distribute_tasks_to_mappers(self, input_splits):
        # Iterate over input splits and available mappers, sending each split to a mapper
        for split, mapper_stub in zip(input_splits, self.mapper_stubs):
            try:
                # Assuming the split is a filename or similar identifier
                input_file = split
                output_file = f"{input_file}_mapped"
                response = mapper_stub.Map(mapper_pb2.MapRequest(input_file=input_file, output_file=output_file))
                if response.status:
                    print(f"Mapping task completed successfully for {input_file}")
                else:
                    print(f"Mapping task failed for {input_file}: {response.message}")
            except grpc.RpcError as e:
                print(f"Failed to distribute task to mapper: {str(e)}")

    def collect_intermediate_data(self, mapper_outputs):
        intermediate_data = []
        for output_file in mapper_outputs:
            with open(output_file, 'r') as infile:
                for line in infile:
                    key, value = line.strip().split('\t')
                    intermediate_data.append((key, value))
        return intermediate_data

    def shuffle_and_sort(self, intermediate_data):
        shuffled_data = {}

        # Organize data by key
        for key, value in intermediate_data:
            if key in shuffled_data:
                shuffled_data[key].append(value)
            else:
                shuffled_data[key] = [value]

        # Optionally sort the keys and/or values if necessary
        # For example, sorting the values for each key if your reduce logic requires it
        for key in shuffled_data:
            shuffled_data[key].sort()

        return shuffled_data

    def NotifyMapperCompletion(self, request, context):
        self.mapper_outputs.append(request.output_file)
        print(f"Mapper completion notified with output file: {request.output_file}")
        
        # Check if all mappers are done and trigger shuffle and sort
        if len(self.mapper_outputs) == len(self.mapper_stubs):
            intermediate_data = self.collect_intermediate_data(self.mapper_outputs)
            shuffled_data = self.shuffle_and_sort(intermediate_data)
            self.distribute_tasks_to_reducers(shuffled_data)
            # Proceed with collecting final results and finalizing the job
        
        return coordinator_pb2.NotificationResponse(message="Notification received")
    
    def NotifyReducerCompletion(self, request, context):
            # Add the reducer's output file location to the list
            self.reducer_outputs.append(request.output_file)
            print(f"Reducer completion notified with output file: {request.output_file}")

            # Check if all reducers are done
            if len(self.reducer_outputs) == len(self.reducer_stubs):
                # All reducers have completed their tasks, proceed to collect and finalize results
                final_results = self.collect_final_results()

                # Finalize the job with the aggregated results
                self.finalize_job(final_results)

                return coordinator_pb2.NotificationResponse(message="Reducer completion acknowledged")
            else:
                return coordinator_pb2.NotificationResponse(message="Reducer completion recorded")
        
    def distribute_tasks_to_reducers(self, shuffled_data):
        # Determine the number of keys each reducer should handle, assuming uniform distribution
        keys_per_reducer = len(shuffled_data) // len(self.reducer_stubs)
        if len(shuffled_data) % len(self.reducer_stubs) != 0:
            keys_per_reducer += 1  # Handle remainder if keys don't divide evenly

        # Convert shuffled_data into a list of items to easily slice it
        items = list(shuffled_data.items())
        
        for i, reducer_stub in enumerate(self.reducer_stubs):
            # Determine the slice of data this reducer should handle
            start_index = i * keys_per_reducer
            end_index = start_index + keys_per_reducer
            reducer_data = items[start_index:end_index]

            # Convert the slice back into a dictionary
            reducer_input = {k: v for k, v in reducer_data}

            # Serialize the reducer_input data as needed and send it to the reducer
            serialized_data = json.dumps(reducer_input)

            # Call the Reduce method on the reducer service
            response = reducer_stub.Reduce(reduce_pb2.ReduceRequest(data=serialized_data))

            # Handle the response from the reducer
            if response.status:
                print(f"Reducer task {i} completed successfully")
            else:
                print(f"Reducer task {i} failed: {response.message}")


    def collect_final_results(self):
        # Placeholder for the final combined results
        final_results = {}

        # Assuming we have a list of reducer output file locations stored in self.reducer_outputs
        for output_file in self.reducer_outputs:
            try:
                with open(output_file, 'r') as infile:
                    for line in infile:
                        key, value = line.strip().split('\t')
                        # Assuming integer values that need to be summed up
                        if key in final_results:
                            final_results[key] += int(value)
                        else:
                            final_results[key] = int(value)
            except Exception as e:
                print(f"Error reading reducer output file {output_file}: {e}")

        # At this point, final_results contains the aggregated data from all reducers
        # You can then write this to a final output file, or return it directly if the coordinator is called programmatically
        with open('final_output.txt', 'w') as outfile:
            for key, value in final_results.items():
                outfile.write(f"{key}\t{value}\n")

        print("Final results collected and written to final_output.txt")
        return final_results  # Or handle the final results as needed
    
    def finalize_job(self, final_results):
        # Example: Write the final results to a file
        try:
            final_output_path = 'final_output.txt'
            with open(final_output_path, 'w') as f:
                for key, value in final_results.items():
                    f.write(f"{key}\t{value}\n")
            print(f"Final results written to {final_output_path}")
        except IOError as e:
            print(f"Failed to write final results: {e}")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    coordinator_pb2_grpc.add_CoordinatorServiceServicer_to_server(CoordinatorService(), server)
    server.add_insecure_port('[::]:50070')
    server.start()
    print(f"Started on port: 50070")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()