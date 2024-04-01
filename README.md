# mapReduce and mongoDB

## Description

This project aims to understand Google's gRPC by implementing mappers and reducers as servers, featuring a heartbeat health check between them. The mapper begins processing the input data and forwards it to the reducer to complete the task. The reducer then stores the data in MongoDB hosted on the cloud. This setup allows for robust data processing and storage, simulating a distributed computing environment.

## Installation

### Prerequisites

- Python 3.7 or higher
- gRPC and related dependencies

### Setup

To install the required dependencies, please follow the links below:

- [Python Installation Guide](https://www.python.org/downloads/)
- [gRPC Installation Guide](https://grpc.io/docs/languages/python/quickstart/)

Ensure all dependencies are installed before proceeding with the usage steps.

## Usage/Workflow

1. **Start the Services**: Run `client.py`, `mapper.py`, and `reducer.py`.
2. **Client Interaction**: Use `client.py` to connect to the mapper. Input the names of the input and output files via the command line interface. The client sends these names to the mapper and prints out messages if errors occur.
3. **Mapper Process**: The mapper, after receiving the file names, processes the input text file and writes the results into a temporary file, which is then inserted into the MongoDB cluster. The path of this temporary file is passed on to the reducer.
4. **Reducer Process**: The reducer fetches the temporary file from MongoDB, performs data reduction by counting each key, and writes the results into the specified output file. The final output is also stored in the MongoDB cluster. The reducer then communicates the success or error status back to the mapper, which relays this information to the client.
5. **Heartbeat Checks**: Throughout the process, heartbeat checks are performed between primary and secondary mappers and reducers to ensure system health and connectivity.

## Contributing

This project is the result of a collaborative educational effort. If you're interested in contributing, feel free to fork the repository and submit a pull request with your proposed changes.

## Contact

For further inquiries or support, please reach out through our [GitHub repository](https://github.com/PlasmaBird/mapReduceProject).

