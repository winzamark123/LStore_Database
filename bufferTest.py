import lstore.config as Config
from lstore.bufferpool import Bufferpool

# Setup for testing - ensure these directories and files exist and are correct for your environment
test_base_path = './testingDir'  # Change this to a valid path for your test
num_columns = 5  # Adjust based on your schema

# Sample record information, adjust according to your actual data structure
sample_record_info = {
    "page_range_num": 1,
    "page_type": "base",  # Or "tail" depending on your implementation
    "page_num": 0
}

def test_bufferpool():
    print("Creating Bufferpool instance...")
    bp = Bufferpool()

    # Insert frame and check capacity
    print("Inserting frame into bufferpool...")
    frame_index = bp.insert_frame(f"{test_base_path}/page_range1/basepage", num_columns, sample_record_info)
    print(f"Frame inserted at index {frame_index}.")

    # Check if record is now in buffer
    if bp.is_record_in_buffer(sample_record_info) != -1:
        print("Record is correctly identified in the buffer.")
    else:
        print("Error: Record not found in buffer after insertion.")

    # Force bufferpool to exceed its capacity to trigger eviction
    print("Forcing eviction by exceeding capacity...")
    for i in range(Config.BUFFERPOOL_FRAME_SIZE + 1):  # Insert more frames than the buffer can hold
        bp.insert_frame(f"{test_base_path}/page_range{i}/", num_columns, {
            "page_range_num": i,
            "page_type": "base",  # Change as needed
            "page_num": i
        })

    # Check again if the original record is still in buffer (it may have been evicted)
    if bp.is_record_in_buffer(sample_record_info) == -1:
        print("Original record was correctly evicted due to capacity constraints.")
    else:
        print("Original record is still in the buffer after eviction. Check eviction policy.")

    print("Bufferpool test completed.")

if __name__ == "__main__":
    test_bufferpool()
