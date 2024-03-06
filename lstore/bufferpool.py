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

    def evict_frame(self)->None:
        #TODO: save the empty frame index
        #TODO: save the frame to disk
        pass

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
