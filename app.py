import random
from fastapi import FastAPI, HTTPException
from pymongo import AsyncMongoClient, ReadPreference
from pymongo.write_concern import WriteConcern
import string

def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


uri = "mongodb+srv://jtrej7_db_user:577F6F6fKw8YEDTm@cluster0.eihhejx.mongodb.net/?appName=Cluster0"
client = AsyncMongoClient(uri)

try:
    database = client.get_database("ev_db")
    vehicles = database.get_collection("vehicles")
except Exception as e:
    raise Exception(f"Unable to get vehicles due to an error: {e}")

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

# Fast but Unsafe Write
@app.post("/insert-fast", status_code=201)
async def fast_write_ev():
    try:
        vehicles_fast_write = vehicles.with_options(write_concern=WriteConcern(w=1))
        result = await vehicles_fast_write.insert_one({
                "VIN (1-10)": random_string(10),
                "County": random_string(50),
                "City": random_string(50),
                "State": random_string(2),
                "Postal Code": random_string(5),
                "Model Year": random_string(4),
                "Make": random_string(50),
                "Model": random_string(50),
                "Electric Vehicle Type": random_string(50),
                "Clean Alternative Fuel Vehicle (CAFV) Eligibility": random_string(50),
                "Electric Range": random_string(50),
                "Legislative District": random_string(50),
                "DOL Vehicle ID": random_string(50),
                "Vehicle Location": random_string(50),
                "Electric Utility": random_string(50),
                "2020 Census Tract": random_string(50),
            })
        assert result.inserted_id is not None
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
# Highyl Durable Write
@app.post("/insert-safe", status_code=201)
async def safe_write_ev():
    try:
        vehicles_safe_write = vehicles.with_options(write_concern=WriteConcern(w="majority"))
        result = await vehicles_safe_write.insert_one({
                "VIN (1-10)": random_string(10),
                "County": random_string(50),
                "City": random_string(50),
                "State": random_string(2),
                "Postal Code": random_string(5),
                "Model Year": random_string(4),
                "Make": random_string(50),
                "Model": random_string(50),
                "Electric Vehicle Type": random_string(50),
                "Clean Alternative Fuel Vehicle (CAFV) Eligibility": random_string(50),
                "Electric Range": random_string(50),
                "Legislative District": random_string(50),
                "DOL Vehicle ID": random_string(50),
                "Vehicle Location": random_string(50),
                "Electric Utility": random_string(50),
                "2020 Census Tract": random_string(50),
            })
        assert result.inserted_id is not None
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# strongly consistent read endpoint
@app.get("/count-tesla-primary")
async def get_tesla_counts():
    try:
        vehicles_strong = vehicles.with_options(read_preference=ReadPreference.PRIMARY)
        count = await vehicles_strong.count_documents({"Make": "TESLA"})
        return {
            "count": count
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# eventually consistent analytical read endpoint
@app.get("/count-bmw-secondary")
async def get_bmw_counts():
    try:
        vehicles_weak = vehicles.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
        count = await vehicles_weak.count_documents({"Make": "BMW"})
        return { "count": count }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
