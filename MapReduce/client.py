from concurrent import futures
import grpc
import mapper_pb2
import mapper_pb2_grpc

MAPPER_ADDRESS = "localhost:50051"

def run():
    #INFO - can get comment out the next 2 lines if you don't want to delete the file
    while True:
        try:
            with grpc.insecure_channel(MAPPER_ADDRESS) as channel:
                stub = mapper_pb2_grpc.MapperServiceStub(channel)
                inputFileName = input("Please enter input file name with .txt or 'exit' to quit: ")
                if inputFileName.lower() == 'exit':
                    break
                outputFileName = input("Please enter output file name with .txt or 'exit' to quit: ")
                if outputFileName.lower() == 'exit':
                    break  

                response = stub.Map(mapper_pb2.MapRequest(input_file=inputFileName, output_file=outputFileName))
                if response.status:
                     print(f"MapReduce completed successfully. Output file: {outputFileName}")
                else:
                    print(f"An error occurred: {response.message}")
        except grpc.RpcError:
            print("MapReduce is currently unavailable. Please try again later.")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == '__main__':
    run()

