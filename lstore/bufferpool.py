import lstore.config as Config
from lstore.record import Record, RID
from lstore.frame import Frame
from lstore.disk import DISK

class Bufferpool:
    def __init__(self):
        self.frames:dict[int,Frame] = {}
        self.frame_info:dict        = {}
        self.frame_count:int        = 0
        self.empty_frame_index:int  = self.frame_count
        self.empty_frame_indices: list = []  # Track empty frame indices

        
    def __has_capacity(self) -> bool:
        print(f"checking capacity with frame count {self.frame_count} and capacity is {self.frame_count < 25}")
        return self.frame_count < 25#Config.BUFFERPOOL_FRAME_SIZE

    def is_record_in_buffer(self, rid:RID, page_type:str, page_index:int)->bool:
        record_key = (
            rid.get_page_range_index(),
            page_type,
            page_index
        )

        if record_key in self.frame_info:
            return True
        else:
            return False


    #returns frame_index
    def get_frame_index(self, rid:RID, page_type:str, page_index:int)->int: #returns frame_index 
        record_key = (
            rid.get_page_range_index(),
            page_type,
            page_index
        )
        if not record_key in self.frame_info:
            raise ValueError

        return self.frame_info[record_key]
    
    #returns record
    def get_record_from_buffer(self, rid: RID, frame_index: int, key_index: int) -> Record:
        if not frame_index in self.frames:
            raise ValueError
        return self.frames[frame_index].get_record(rid=rid, key_index=key_index)

    #     page_range_info = record_info["page_range_num"]
    #     page_type_info = record_info["page_type"]
    #     page_num_info = record_info["page_num"]
    #     record_key = (page_range_info, page_type_info, page_num_info)

    #     if record_key in self.frame_info:
    #         print("Record is in bufferpool")
    #         frame_index = self.frame_info[record_key]
    #         return frame_index
    #     return -1

    # Function that finds the frame that has the longest time in the bufferpool that is not pinned
    def evict_frame(self)->None:
        print("evicting frame")
        lru_time_frame = None
        lru_frame_index = 0

        # Find the least recently used frame that is not pinned
        for frame_index, frame_obj in self.frames.items():
            print(f'Frame index: {frame_index} with time {frame_obj.last_time_used}')
            if not frame_obj.is_pin:
                if lru_time_frame is None or frame_obj.last_time_used < lru_time_frame.last_time_used:
                    lru_time_frame = frame_obj
                    lru_frame_index = frame_index

        if lru_time_frame is not None:
            print(f"The frame with the highest last_time_used and not pinned is Frame {lru_time_frame} with frame index : {lru_frame_index} and path to page {lru_time_frame.path_to_page}")
        else:
            print("No frame is currently not pinned.") 


    # IF THE FRAME IS DIRTY, IT WRITES IT TO THE DISK
        if lru_frame_index and self.frames[lru_frame_index].is_dirty:
            # Save the frame's data back to disk before eviction

            # path to page range to write to 
            path_to_physical_page = self.frames[lru_frame_index].path_to_page

            i = 0
            for physical_page in self.frames[lru_frame_index].physical_pages:
                print(f'Evicting frame index: {lru_frame_index} path pages to {path_to_physical_page} for index {i}')
                DISK.write_physical_page_to_disk(path_to_physical_page=path_to_physical_page, physical_page=physical_page, page_index=i)
                i += 1

            self.frames[lru_frame_index].set_clean()
        
        # Remove the frame from the buffer pool and directory
        if lru_frame_index:
            del self.frames[lru_frame_index]
            for key,value in self.frame_info.items():
                if value == lru_frame_index:
                    key_delete = key
            del self.frame_info[key_delete]

            # Update empty frame indices
            self.empty_frame_indices.append(lru_frame_index)

        # Update empty frame index
        if self.empty_frame_indices:
            self.empty_frame_index = self.empty_frame_indices[0]
        else:
            self.empty_frame_index = None

        # updates indexes for frames
        self.frame_count -= 1

        print(f"Finished evicting, new frame count {self.frame_count}")


    def import_frame(self, path_to_page: str, num_columns: int, record_info: dict) -> int:
        if not self.__has_capacity():
            self.evict_frame()
            self.frame_count += 1
            frame_index = self.empty_frame_index
        else: 
            self.frame_count += 1
            frame_index = self.frame_count
            # self.empty_frame_index += 1

        print(f"frame_index for frame: {frame_index}")
        self.frame_count += 1
        self.frames[frame_index] = Frame(path_to_page= path_to_page)
        self.frames[frame_index].load_data(num_columns=num_columns, path_to_page=path_to_page) 

        page_range_info = record_info["page_range_num"]
        page_type_info = record_info["page_type"]
        page_index_info = record_info["page_index"]
        record_key = (page_range_info, page_type_info, page_index_info)

        self.frame_info[record_key] = frame_index
        
        return frame_index
    
    def insert_record(self, key_index:int, frame_index:int, record:Record) -> None:
        print("INSERT BUFFER")
        self.frames[frame_index].insert_record(key_index=key_index, record=record)

    def update_record(self, rid:RID, frame_index:int, new_record:Record)->None:
        pass
    
    
BUFFERPOOL = Bufferpool()
