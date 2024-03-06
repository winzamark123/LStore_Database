import lstore.config as Config
from lstore.record import Record, RID
from lstore.frame import Frame
from lstore.disk import DISK

class Bufferpool:
    def __init__(self):
        self.frames:dict[tuple[str,int],Frame] = {} # {(page_path, frame_index): Frame}

        self.frame_count:int        = 0
        self.empty_frame_index: int = -1



    def __has_capacity(self) -> bool:
        print(f"checking capacity with frame count {self.frame_count} and capacity is {self.frame_count < 2}")
        return self.frame_count < Config.BUFFERPOOL_FRAME_SIZE

    def __is_record_in_buffer(self, rid:RID, page_path:str)->bool:
        return page_path in {_[0] for _ in self.frames.keys()}:




    #returns frame_index
    def __get_frame_index(self, rid:RID, page_index:int)->int: #returns frame_index
        record_key = (
            rid.get_page_range_index(),
            page_index
        )
        if not record_key in self.frame_info:
            raise ValueError

        return self.frame_info[record_key]

    #returns data
    def get_data_from_buffer(self, rid: RID, frame_index: int) -> tuple: #return data
        if not frame_index in self.frames:
            raise ValueError
        return self.frames[frame_index].get_data(rid=rid)

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

        # updates indexes for frames
        self.__update_frame_indexes(delete_index=frame_index)
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

        self.frames[frame_index] = Frame(path_to_page= path_to_page)
        
        print(f'Frame Time: {self.frames[frame_index].time_in_buffer}')
        self.frames[frame_index].load_data(num_columns=num_columns, path_to_page=path_to_page)



        page_range_info = record_info["page_range_index"]
        page_index_info = record_info["page_index"]
        record_key = (page_range_info, page_index_info)

        self.frame_info[record_key] = frame_index

        return frame_index

    def insert_record(self, key_index:int, frame_index:int, record:Record) -> None:
        print("INSERT BUFFER")
        self.frames[frame_index].insert_record(key_index=key_index, record=record)
        self.frames[frame_index].set_dirty()

    def update_record(self, rid:RID, frame_index:int, new_record:Record)->None:
        pass


BUFFERPOOL = Bufferpool()
