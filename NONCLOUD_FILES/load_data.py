# Script to load Electric Vehicle CSV data into MongoDB using batch insertion
import csv
from pymongo import MongoClient
from dotenv import load_dotenv	
import os

# MongoDB connection
load_dotenv()
client = MongoClient(os.getenv('MONGO_DB_STR'))
db = client['ev_db']
collection = db['vehicles']

csv_file = 'dataset/Electric_Vehicle_Population_Data.csv'

BATCH_SIZE = 1000

def batch_insert_csv(csv_path, collection, batch_size=1000):
	with open(csv_path, newline='', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		batch = []
		for row in reader:
			batch.append(row)
			if len(batch) >= batch_size:
				collection.insert_many(batch)
				batch = []
		if batch:
			collection.insert_many(batch)

if __name__ == '__main__':
	batch_insert_csv(csv_file, collection, BATCH_SIZE)
	print('Data loaded successfully into ev_db.vehicles')
