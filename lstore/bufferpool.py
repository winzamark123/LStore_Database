import lstore.config as Config
from lstore.frame import Frame
from lstore.disk import DISK

class Bufferpool:
    def __init__(self):
        self.frames:dict          = {}
        self.frame_info:dict      = {}
        self.frame_count:int      = 0
    
    def __has_capacity(self) -> bool:
        return self.frame_count < Config.BUFFERPOOL_FRAME_SIZE

    def is_record_in_buffer(self, record_info: dict) -> int:
        page_range_info = record_info["page_range_num"]
        page_type_info = record_info["page_type"]
        page_num_info = record_info["page_num"]
        record_key = (page_range_info, page_type_info, page_num_info)

        if record_key in self.frame_info:
            print("Record is in bufferpool")
            frame_index = self.frame_info[record_key]
            return frame_index
        return -1

    def evict_frame(self)->None:
    # Find the least recently used frame that is not pinned
    # Function that finds the frame that has the longest time in the bufferpool that is not pinned
        lru_frame_key, lru_frame = min(
            ((key, frame) for key, frame in self.frames.items() if not frame.is_pin), 
            key=lambda item: item[1].time_in_bufferpool, default=(None, None)
        )
    
    # IF THE FRAME IS DIRTY, IT WRITES IT TO THE DISK
        if lru_frame and lru_frame.is_dirty:
            # Save the frame's data back to disk before eviction
            i = 0
            for physical_page in lru_frame.physical_pages:
                path_to_physical_page = f"{lru_frame.path_to_page}{i}"
                # Pseudocode for saving to disk
                DISK.write_physical_page_to_disk(path_to_physical_page, physical_page)
                i += 1

            lru_frame.set_clean()
        
        # Remove the frame from the buffer pool and directory
        if lru_frame_key:
            del self.frames[lru_frame_key]
            del self.frame_info[lru_frame_key]

        self.frame_count -= 1

    def insert_frame(self, path_to_page: str, num_columns: int, record_info: dict) -> int:
        if not self.__has_capacity():
            self.evict_frame()

        self.frame_count += 1
        frame_index = self.frame_count
        self.frames[frame_index] = Frame(path_to_page= path_to_page)
        self.frames[frame_index].load_data(num_columns=num_columns, path_to_page=path_to_page) 

        page_range_info = record_info["page_range_num"]
        page_type_info = record_info["page_type"]
        page_num_info = record_info["page_num"]
        record_key = (page_range_info, page_type_info, page_num_info)

        self.frame_info[record_key] = frame_index
        
        return frame_index
    
BUFFERPOOL = Bufferpool()
