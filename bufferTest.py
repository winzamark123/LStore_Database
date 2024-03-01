# Test script for Bufferpool
import lstore.config as Config
from lstore.bufferpool import BUFFERPOOL

# Settings for the test
Config.BUFFERPOOL_FRAME_SIZE = 3  # Set to a small number for testing
test_base_path = '/test'  # Adjust this path
num_columns = 5  # Change as per your table schema

# Sample record information for testing
sample_record_info = {
    "page_range_num": 0,
    "page_type": "base",
    "page_num": 0
}

# Function to test Bufferpool functionality
def test_bufferpool():
    print("Creating a Bufferpool instance...")

    # Insert a frame and check if it's in the buffer
    print("Inserting a frame into the buffer pool...")
    frame_index = BUFFERPOOL.insert_frame(f"{test_base_path}/PR0/BP0", num_columns, sample_record_info)
    print(f"Frame inserted with index {frame_index}.")

    # Verify the frame is in the buffer
    in_buffer = BUFFERPOOL.is_record_in_buffer(sample_record_info)
    if in_buffer != -1:
        print("Record found in the buffer pool.")
    else:
        print("Error: Record not found in buffer after insertion.")

    # Insert more frames to exceed capacity and trigger eviction
    print("Exceeding buffer pool capacity to trigger eviction...")
    for i in range(1, Config.BUFFERPOOL_FRAME_SIZE + 2):  # Add enough frames to exceed capacity
        new_record_info = {"page_range_num": i, "page_type": "base", "page_num": 0}
        BUFFERPOOL.insert_frame(f"{test_base_path}/page_range{i}/", num_columns, new_record_info)

    # Check if the original record was evicted
    if BUFFERPOOL.is_record_in_buffer(sample_record_info) == -1:
        print("Original record was successfully evicted.")
    else:
        print("Error: Original record is still in the buffer after exceeding capacity.")

    print("Bufferpool test completed.")

# Run the test
if __name__ == "__main__":
    test_bufferpool()
