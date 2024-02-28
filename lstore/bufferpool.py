import lstore.config as Config
from lstore.physical_page import Physical_Page
from lstore.frame import Frame
import os 

class Bufferpool:
    def __init__(self, db_name: str, table_name: str):
        self.frame_object = {}
        self.frame_directory= {}
        self.frame_count = 0
        self.table_name = table_name
        self.path_to_table = os.getcwd() + '/' + db_name + '/' + table_name
        self.merge_buffer = False 

    
    def __has_capacity(self) -> bool:
        return self.frame_count < Config.BUFFERPOOL_FRAME_SIZE

    def is_record_in_buffer(self, record_info: dict) -> int:
        page_range_info = record_info["page_range_num"]
        base_page_info = record_info["base_page_num"]
        record_key = (page_range_info, base_page_info)

        if record_key in self.frame_directory:
            print("Record is in bufferpool")
            frame_index = self.frame_directory[record_key]
            return frame_index
        return -1


    def has_capacity(self):
        return self.cur_size <= self.max_size
    
    def evict_frame(self):
    # Find the least recently used frame that is not pinned
    # Function that finds the frame that has the longest time in the bufferpool that is not pinned
        lru_frame = min((frame for frame in self.frame_object if not frame.is_pin), 
                        key=lambda f: f.time_in_bufferpool, default=None)
    
    # IF THE FRAME IS DIRTY, IT WRITES IT TO THE DISK
        if lru_frame and lru_frame.dirty_bit:
            # Save the frame's data back to disk before eviction
            # Pseudocode for saving to disk
            # write_path = last_used_page.path_to_page_on_disk
            # all_cols = last_used_page.all_columns
            # save_to_disk_physicalPage(write_path, all_cols)
            lru_frame.set_clean()
        
        # Remove the frame from the buffer pool and directory
        if lru_frame:
            frame_index = self.frame_object.index(lru_frame)
            del self.frame_object[frame_index]
            del self.frame_directory[lru_frame.tuple_key]

    def __import_frame(self, path_to_page: str, table_name: str):
        self.frame_count += 1
        self.frame_object[self.frame_count] = Frame(path_to_page= path_to_page, table_name= table_name)

    def load_frame_to_buffer(self, path_to_page: str, table_name: str, num_columns: int, record_info: dict):
        if not self.__has_capacity():
            self.evict_frame()

        self.__import_frame(path_to_page=path_to_page, table_name=table_name)
        
        frame_index = self.frame_count

        # Pin the frame
        self.frame_object[frame_index].pin_frame()

        data_entry_size = Config.DATA_ENTRY_SIZE

        for i in range(num_columns + Config.META_DATA_NUM_COLUMNS):
            # Ensure each physical page is appended correctly within the loop
            self.frame_object[frame_index].physical_pages.append(Physical_Page(entry_size=data_entry_size, column_number=i))
            
            # Construct the path for each physical page within the loop
            path_to_physical_page = f"{path_to_page}/{i}.bin"  # Use formatted string for clarity

            # Ensure directory exists before attempting to read or write
            os.makedirs(os.path.dirname(path_to_physical_page), exist_ok=True)
            print("PATH_TO_PHYUSICALSDALK", path_to_physical_page)

            # Check if the file exists to decide whether to read from it or initialize a new one
            if os.path.exists(path_to_physical_page):
                self.frame_object[frame_index].physical_pages[i].read_from_disk(path_to_physical_page=path_to_physical_page, column_index=i)
            else:
                # If the file does not exist, you may need to create and initialize it
                # Example: initializing an empty file
                with open(path_to_physical_page, 'wb') as f:
                    # Initialize the file if needed; for example, writing empty bytes:
                    f.write(b'\x00' * data_entry_size)  # Adjust this according to your data structure needs

        # Record frame information
        page_range_info = record_info["page_range_num"]
        base_page_info = record_info["base_page_num"]
        record_key = (page_range_info, base_page_info)
        self.frame_directory[record_key] = frame_index

        return frame_index




    



     