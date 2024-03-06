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
        #TODO: save the empty frame index
        pass


    def __import_frame(self, path_to_page: str, num_columns: int)->None:
        if not self.__has_capacity():
            self.__evict_frame()
            
        self.frames[path_to_page] = Frame(path_to_page= path_to_page)
        print(f'Frame Time: {self.frames[path_to_page].time_in_buffer}')
        self.frames[path_to_page].load_data(num_columns=num_columns, path_to_page=path_to_page)

    def insert_record(self, page_path:str, record:Record)->None:
        if not self.__is_record_in_buffer(page_path):
            self.__import_frame(path_to_page=page_path, num_columns=Config.NUM_COLUMNS)




        self.frames[page_path].insert_record(record=record)

    def get_data_from_buffer(self, rid: RID, page_path:str, num_columns=int)->tuple: #return data
        if not self.__is_record_in_buffer(rid, page_path):
            self.__import_frame(path_to_page=page_path, num_columns=num_columns)
        
        return self.frames[page_path].get_data(rid)

    def update_record(self, rid:RID, new_record:Record)->None:
        pass

    def delete_record(self, rid:RID)->None:
        pass


BUFFERPOOL = Bufferpool()
