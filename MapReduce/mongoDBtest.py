from pymongo.mongo_client import MongoClient

# MongoDB connection string
mongo_uri = "mongodb+srv://admin4459:admin4459@cluster0.qpwu0ez.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Connect to MongoDB
client = MongoClient(mongo_uri)

# Select your database
db = client['4459']

# For text files: Select your collection
collection = db['FILES']

# Insert a text file
def insert_text_file(file_path, file_name):
    # Reading the file as binary
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    with open(file_path, 'r') as file:
        file_content = file.read()

    # Storing the file
    collection.insert_one({'filename': file_name, 'file_data': file_content})
    print(f"Text file {file_name} inserted.")


# Example usage
if __name__ == "__main__":
    # Insert a text file
    insert_text_file('./input.txt', 'input.txt')
