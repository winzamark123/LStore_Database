import lstore.config as Config
from lstore.record import Record, RID
from lstore.frame import Frame
from lstore.disk import DISK

class Bufferpool:
    def __init__(self):
        self.frames:dict[str,Frame] = {} # {page_path: Frame}

    def __has_capacity(self) -> bool:
        return len(self.frames) <= Config.BUFFERPOOL_FRAME_SIZE

    def __is_record_in_buffer(self, page_path:str)->bool:
        return page_path in self.frames



    #returns data

    def __evict_frame(self)->None:
        return 
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

    def __import_frame(self, path_to_page: str, num_columns: int)->None:
        if not self.__has_capacity():
            self.__evict_frame()
            
        self.frames[path_to_page] = Frame(path_to_page= path_to_page)
        self.frames[path_to_page].load_data(num_columns=num_columns, path_to_page=path_to_page)

    def insert_record(self, page_path:str, record:Record, num_columns=int)->None:
        if not self.__is_record_in_buffer(page_path):
            self.__import_frame(path_to_page=page_path, num_columns=num_columns)

        self.frames[page_path].insert_record(record=record)

    def get_data_from_buffer(self, rid: RID, page_path:str, num_columns:int)->tuple: #return data
        if not self.__is_record_in_buffer(page_path):
            self.__import_frame(path_to_page=page_path, num_columns=num_columns)
        
        return self.frames[page_path].get_data(rid)

    def update_record(self, rid:RID, new_record:Record, num_columns:int)->None:
        pass

    def delete_record(self, rid:RID)->None:
        pass


BUFFERPOOL = Bufferpool()
