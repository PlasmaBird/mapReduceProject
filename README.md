# mapReduce and mongoDB

## Description

This project aims to gain more understanding of distributed data processing and Google's gRPC by implementing mappers and reducers as servers, featuring a heartbeat health check between them. The mapper begins processing the input data and forwards it to the reducer to complete the task. The reducer then stores the data in MongoDB hosted on the cloud. This setup allows for robust data processing and storage, simulating a distributed computing environment.

## Installation

### Prerequisites

- Python 3.7 or higher
- gRPC and related dependencies

### Setup

To install the required dependencies, please follow the links below:

- [Python Installation Guide](https://www.python.org/downloads/)
- [gRPC Installation Guide](https://grpc.io/docs/languages/python/quickstart/)

Ensure all dependencies are installed before proceeding with the usage steps.

## Components 
Client and servers: `client.py`, `mapper.py`, `reducer.py`

Proto files: `mapper.proto`, `reducer.proto`

gRPC files: `mapper_pb2_grpc.py`, `reducer_pb2_grpc.py`

Generated protocol buffer code: `mapper_pb2.py`, `reducer_pb2.py`

## Usage/Workflow

1. **Start the Services**:
   ```bash
   python client.py
   python mapper.py
   python reducer.py
2. **Client Interaction**: Use `client.py` to connect to the mapper. Input the names of the input and output files via the command line interface. The client sends these names to the mapper and prints out messages if errors occur.

<details>
  <summary>**Client Input Code**</summary>

```python
inputFileName = input("Please enter input file name with .txt or 'exit' to quit: ")
if inputFileName.lower() == 'exit':
    break
outputFileName = input("Please enter output file name with .txt or 'exit' to quit: ")
if outputFileName.lower() == 'exit':
    break 
```
</details>

3. **Mapper Process**: The mapper, after receiving the file names, processes the input text file. And writes the results into a temporary file, which is then inserted into the MongoDB cluster. The path of this temporary file is passed on to the reducer.

<details>
  <summary>**Mapper Processing Code**</summary>

```python
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
```
</details>

4. **Reducer Process**: The reducer fetches the temporary file from MongoDB, performs data reduction by counting each key, and writes the results into the specified output file. The final output is also stored in the MongoDB cluster. The reducer then communicates the success or error status back to the mapper, which relays this information to the client.

<details>
  <summary>**Reducer Processing Code**</summary>

```python
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
```
</details>

5. **Heartbeat Checks**: Throughout the process, heartbeat checks are performed between primary and secondary mappers and reducers to ensure system health and connectivity.

**add heartbeat code snip here**

**add mongoDB related workflow here with some examples/codeSnip**

## Logic Behind Design Choices

## Challenges

## Testing

## Contributing

This project is the result of a collaborative educational effort. If you're interested in contributing, feel free to fork the repository and submit a pull request with your proposed changes.

## Contact

For further inquiries or support, please reach out through our [GitHub repository](https://github.com/PlasmaBird/mapReduceProject).

