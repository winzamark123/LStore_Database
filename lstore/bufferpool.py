import lstore.db as Database
import lstore.table as Table
import lstore.frame as Frame
import lstore.config as Config
import lstore.physical_page as Physical_Page

class Bufferpool():
    def __init__(self, db:Database, table:Table):
        self.db = db
        self.table = table
        self.cur_size = 0
        self.max_size = Config.BUFFER_SIZE
        self.frame_list = []
        self.page_list = []
    
    def new_frame(self):
        if self.has_capacity():
            self.import_frame()
        else: 
            self.evict_frame()
            self.import_frame()

    def has_capacity(self):
        return self.cur_size <= self.max_size
    
    # def import_frame(self, page:Page):
    #     frame = Frame(page)
    #     self.frame_list.append(frame)
    #     self.page_list.append(page)
    #     self.cur_size += 1
    #     return frame

    def import_frame(self, physical_page: Physical_Page):
        self.cur_size += 1
        frame = Frame(self.db.disk[self.table].load_page())

        pass
    
    def evict_frame(self):

        pass 



     