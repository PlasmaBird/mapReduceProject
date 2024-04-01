from pymongo.mongo_client import MongoClient
import sys

# MongoDB connection string
mongo_uri = "mongodb+srv://admin4459:admin4459@cluster0.qpwu0ez.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Connect to MongoDB
client = MongoClient(mongo_uri)

# Select your database
db = client['4459']

# For text files: Select your collection
input_collection = db['INPUT_FILES']
output_collection = db['OUTPUT_FILES']
# Insert a text file
def insertFileInMongoDB(file_to_store, isInput):
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        with open(file_to_store, 'r') as file:
            file_content = file.read()

        if isInput == 'True':
            collection = input_collection
        else:
            collection = output_collection
    # Find the highest counter value in the collection
        last_document = collection.find_one(sort=[("file_id", -1)])  # Sort documents by counter in descending order
        # If the collection is empty, start the counter at 1; otherwise, increment the counter by 1
        new_counter = 1 if last_document is None else last_document['file_id'] + 1
        # Storing the file
        collection.insert_one({'file_id': new_counter, 'filename': file_to_store, 'file_data': file_content})
        return True
    except Exception as e:
        print(str(e))
        return False


# Example usage
if __name__ == "__main__":
    # Insert a text file
    file_to_store = sys.argv[1]
    isInput = sys.argv[2]
    insertFileInMongoDB(file_to_store, isInput)
