from fastapi import FastAPI, HTTPException, Query
from pymongo import AsyncMongoClient
from pymongo.read_preferences import Primary, SecondaryPreferred
from pymongo.write_concern import WriteConcern
from pydantic import BaseModel, Field
from typing import Annotated

class WriteParams(BaseModel):
    model_config =  {"extra": "forbid"}

    vin: str = Field(max_length = 10)
    county: str = Field()
    city: str = Field()
    state: str = Field(max_length = 2, to_upper=True)
    postal_code: str = Field(min_length=5,max_length=5)
    model_year: str = Field(min_length=4,max_length=4)
    make: str = Field()
    model: str = Field()
    elec_vehicle_type: str = Field()
    cafv_eligibility: str = Field()
    ev_rng: str = Field()
    lgs_dist: str = Field()
    dol_veh_id: str = Field()
    veh_loc: str = Field()
    eu: str = Field()
    census_track: str = Field()

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
async def fast_write_ev(write_query: Annotated[WriteParams, Query()]):
    try:
        vehicles_fast_write = vehicles.with_options(write_concern=WriteConcern(w=1))
        result = await vehicles_fast_write.insert_one({
                "VIN (1-10)": write_query.vin,
                "County": write_query.county,
                "City": write_query.city,
                "State": write_query.state,
                "Postal Code": write_query.postal_code,
                "Model Year": write_query.model_year,
                "Make": write_query.make,
                "Model": write_query.model,
                "Electric Vehicle Type": write_query.elec_vehicle_type,
                "Clean Alternative Fuel Vehicle (CAFV) Eligibility": write_query.cafv_eligibility,
                "Electric Range": write_query.ev_rng,
                "Legislative District": write_query.lgs_dist,
                "DOL Vehicle ID": write_query.dol_veh_id,
                "Vehicle Location": write_query.veh_loc,
                "Electric Utility": write_query.eu,
                "2020 Census Tract": write_query.census_track,
            })
        assert result.inserted_id is not None
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
# Highyl Durable Write
@app.post("/insert-safe", status_code=201)
async def safe_write_ev(write_query: Annotated[WriteParams, Query()]):
    try:
        vehicles_safe_write = vehicles.with_options(write_concern=WriteConcern(w="majority"))
        result = await vehicles_safe_write.insert_one({
                "VIN (1-10)": write_query.vin,
                "County": write_query.county,
                "City": write_query.city,
                "State": write_query.state,
                "Postal Code": write_query.postal_code,
                "Model Year": write_query.model_year,
                "Make": write_query.make,
                "Model": write_query.model,
                "Electric Vehicle Type": write_query.elec_vehicle_type,
                "Clean Alternative Fuel Vehicle (CAFV) Eligibility": write_query.cafv_eligibility,
                "Electric Range": write_query.ev_rng,
                "Legislative District": write_query.lgs_dist,
                "DOL Vehicle ID": write_query.dol_veh_id,
                "Vehicle Location": write_query.veh_loc,
                "Electric Utility": write_query.eu,
                "2020 Census Tract": write_query.census_track,
            })
        assert result.inserted_id is not None
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# strongly consistent read endpoint
@app.get("/count-tesla-primary")
async def get_tesla_counts():
    try:
        vehicles_strong = vehicles.with_options(read_preference=Primary())
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
        vehicles_weak = vehicles.with_options(read_preference=SecondaryPreferred())
        count = await vehicles_weak.count_documents({"Make": "BMW"})
        return { "count": count }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
