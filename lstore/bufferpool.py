import lstore.db as Database
import lstore.config as Config
import lstore.table as Table
import lstore.frame as Frame
import lstore.config as Config
import lstore.physical_page as Physical_Page
import os 

class Bufferpool:
    def __init__(self, table_name: str):
        self.frame_object = {}
        self.frame_directory= {}
        self.frame_count = 0
        self.table_name = table_name
        self.path_to_table = os.getcwd() + '/' + table_name
        self.merge_buffer = False 

    
    def __has_capacity(self) -> bool:
        return self.frame_count < Config.BUFFERPOOL_FRAME_SIZE

    def is_record_in_buffer(self, record_info: dict) -> bool:
        page_range_info = record_info["page_range_num"]
        base_page_info = record_info["base_page_num"]

        if page_range_info in self.frame_object:
            if base_page_info in self.frame_object[page_range_info]:
                return True
        return False

    def __import_frame(self, path_to_page: str, table_name: str):
        self.frame_count += 1
        self.frame_object[self.frame_count] = Frame(path_to_page= path_to_page, table_name= table_name)

    def has_capacity(self):
        return self.cur_size <= self.max_size
    
    def evict_frame(self):
        pass 

    def load_frame(self, path_to_page: str, table_name: str, num_columns: int):
        if self.__has_capacity():
            self.__import_frame(path_to_page= path_to_page, table_name= table_name)
        else:
            self.evict_frame()
            self.__import_frame(path_to_page= path_to_page, table_name= table_name)
        
        frame_index = self.frame_count

        #pin the frame
        self.frame_object[frame_index].pin_frame()

        data_entry_size = Config.DATA_ENTRY_SIZE
        self.frame_object[frame_index].physical_pages = [Physical_Page(entry_size=data_entry_size) for i in range(num_columns)]

        #frame_object{
        #   1: Frame()
        #}

        for i in range(num_columns):
            path_to_physical_page = path_to_page + '/' + str(i) + '.bin'
            self.frame_object[frame_index].physical_pages[i].read_from_disk(path_to_physical_page=path_to_physical_page, column_index=i) 

        return frame_index




    



     