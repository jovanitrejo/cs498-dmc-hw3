from pydantic import BaseModel, Field
import requests
import time
import random
import string
import statistics

server_uri: str = "http://34.59.113.0:8000"


class WriteParams(BaseModel):
    model_config = {"extra": "forbid"}

    vin: str = Field(max_length=10)
    county: str
    city: str
    state: str = Field(min_length=2, max_length=2)
    postal_code: str = Field(min_length=5, max_length=5)
    model_year: str = Field(min_length=4, max_length=4)
    make: str
    model: str
    elec_vehicle_type: str
    cafv_eligibility: str
    ev_rng: str
    lgs_dist: str
    dol_veh_id: str
    veh_loc: str
    eu: str
    census_track: str


# Generate random strings for testing
def random_string(length: int) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

# Generate random parameters for testing, uses random_string() to create random data for each field in WriteParams. The lengths of the strings are chosen to match the constraints defined in the WriteParams model.
def generate_random_params() -> WriteParams:
    return WriteParams(
        vin=random_string(10),
        county=random_string(10),
        city=random_string(10),
        state=random_string(2),
        postal_code=random_string(5),
        model_year=random_string(4),
        make=random_string(10),
        model=random_string(10),
        elec_vehicle_type=random_string(10),
        cafv_eligibility=random_string(10),
        ev_rng=random_string(10),
        lgs_dist=random_string(10),
        dol_veh_id=random_string(10),
        veh_loc=random_string(10),
        eu=random_string(10),
        census_track=random_string(10),
    )

# sends POST request to specified endpoint (/insert-fast or /insert-safe) with the given parameters 
# and measures the time taken for the request to complete.
def timed_post(session: requests.Session, endpoint: str, params: WriteParams) -> float | None:
    start_time = time.perf_counter()
    response = session.post(f"{server_uri}{endpoint}", params=params.model_dump())
    end_time = time.perf_counter()

    if response.status_code != 201:
        print(f"Request to {endpoint} failed: {response.status_code} {response.text}")
        return None

    return end_time - start_time


# Summarizes the timing results by calculating and printing the average, median, minimum, and maximum times for a list of successful request times. If there are no successful requests, it prints a message indicating that no data is available for that label.
def summarize_times(label: str, times: list[float]) -> None:
    if not times:
        print(f"No successful requests recorded for {label}.")
        return

    print(f"\n{label}")
    print(f"Successful requests: {len(times)}")
    print(f"Average time: {statistics.mean(times):.4f} seconds")

# Performs a series of warm-up requests to the specified endpoint to ensure that the server is ready and any initial overhead is minimized 
def warmup(session: requests.Session, endpoint: str) -> None:
    for _ in range(10):
        params = generate_random_params()
        session.post(f"{server_uri}{endpoint}", params=params.model_dump())

# Main function performing the test for both the fast and durable endpoints.
def test_writes(num_requests: int = 50) -> None:
    fast_times: list[float] = []
    safe_times: list[float] = []

    with requests.Session() as session:
        print("Running warm-up requests...")
        warmup(session, "/insert-fast")
        warmup(session, "/insert-safe")

        print("Running measured requests...")

        # alternating between two endpoints for each request
        # generating random parameters for writes.
        for i in range(num_requests):
            fast_params = generate_random_params()
            fast_time = timed_post(session, "/insert-fast", fast_params)
            if fast_time is not None:
                fast_times.append(fast_time)

            safe_params = generate_random_params()
            safe_time = timed_post(session, "/insert-safe", safe_params)
            if safe_time is not None:
                safe_times.append(safe_time)

            print(f"Completed pair {i + 1}/{num_requests}")

    summarize_times("Fast write (/insert-fast)", fast_times)
    summarize_times("Durable write (/insert-safe)", safe_times)


if __name__ == "__main__":
    test_writes()