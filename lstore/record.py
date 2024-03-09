import lstore.config as Config

class RID:

    def __init__(self, rid:int)->None:
        self.rid = rid

    def __int__(self)->int:
        return self.rid

    def get_page_range_index(self):
        return (abs(self.rid) - 1) // (Config.RECORDS_PER_PAGE * Config.NUM_BASE_PAGES)

    def get_base_page_index(self):
        return ((abs(self.rid)- 1) // Config.RECORDS_PER_PAGE) % Config.NUM_BASE_PAGES

    def get_pp_index(self):
        return (self.rid % Config.RECORDS_PER_PAGE)

    def to_int(self):
        return self.rid

    def get_tail_page_index(self):
        page_index = (abs(self.rid) - 1) // Config.RECORDS_PER_PAGE
        return page_index


class Record:

    def __init__(self, rid:int, key:int, columns:tuple)->None:
        self.rid = RID(rid)
        self.key = key
        self.columns = columns

    def get_key(self):
        return self.key

    def get_values(self):
        return (self.key,) + tuple(self.columns)
        #123123, 0, 0 ,0, 0

    def get_page_range_index(self):
        return self.rid.get_page_range_index()

    def get_base_page_index(self):
        return self.rid.get_base_page_index()

    def get_rid(self):
        return self.rid.rid

    def get_columns(self):
        return self.columns

