import grpc
import coordinator_pb2  # Assuming you have a proto file for coordinator communication
import coordinator_pb2_grpc

COORDINATOR_ADDRESS = "localhost:50070"  # Address of the coordinator

def run():
    while True:
        try:
            with grpc.insecure_channel(COORDINATOR_ADDRESS) as channel:
                stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
                inputLocation = input("Please enter the location of the input data or 'exit' to quit: ")
                if inputLocation.lower() == 'exit':
                    break

                # Assuming a StartJob method exists in your coordinator service definition
                response = stub.StartJob(coordinator_pb2.JobRequest(input_location=inputLocation))
                if response.status:
                     print("MapReduce job completed successfully.")
                else:
                    print(f"An error occurred: {response.message}")
        except grpc.RpcError:
            print("Coordinator is currently unavailable. Please try again later.")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    run()
