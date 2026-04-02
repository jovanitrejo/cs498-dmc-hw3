import requests
import time
import statistics

server_uri: str = "http://34.59.113.0:8000"

# sends POST request to specified endpoint (/insert-fast or /insert-safe) with the given parameters 
# and measures the time taken for the request to complete.
def timed_post(session: requests.Session, endpoint: str) -> float | None:
    start_time = time.perf_counter()
    response = session.post(f"{server_uri}{endpoint}")
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
        session.post(f"{server_uri}{endpoint}")

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
            fast_time = timed_post(session, "/insert-fast")
            if fast_time is not None:
                fast_times.append(fast_time)

            safe_time = timed_post(session, "/insert-safe")
            if safe_time is not None:
                safe_times.append(safe_time)

            print(f"Completed pair {i + 1}/{num_requests}")

    summarize_times("Fast write (/insert-fast)", fast_times)
    summarize_times("Durable write (/insert-safe)", safe_times)


if __name__ == "__main__":
    test_writes()