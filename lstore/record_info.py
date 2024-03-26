import lstore.config as Config

class RID:

    def __init__(self, rid:int)->None:
        self.rid:int = int(rid)

    def __repr__(self)->str:
        return str(self.rid)

    def __str__(self)->str:
        return str(self.rid)

    def __int__(self)->int:
        return self.rid

    def get_page_range_index(self)->int:
        """
        Get Page Range index rid is in
        """
        return (abs(self.rid) - 1) // (Config.NUM_RECORDS_PER_PAGE * Config.NUM_BASE_PAGES_PER_PAGE_RANGE)

    def get_base_page_index(self)->int:
        """
        Get Base Page index rid is in
        """
        return ((abs(self.rid)- 1) // Config.NUM_RECORDS_PER_PAGE) % Config.NUM_BASE_PAGES_PER_PAGE_RANGE


class TID(RID):

    def __init__(self, rid:int)->None:
        super().__init__(rid)

    def __int__(self)->int:
        return self.rid

    def get_tail_page_index(self)->int:
        """
        Get Tail Page index tid is in
        """
        return (abs(self.rid) - 1) // Config.NUM_RECORDS_PER_PAGE


class Record:

    def __init__(self, rid:int, key_index:int, columns:list)->None:
        self.rid:RID       = RID(rid)
        self.key_index:int = key_index
        self.columns:list = list(columns)

    def __str__(self)->str:
        return f"RID {self.rid}: {self.columns}"

    def get_rid(self)->RID:
        """
        Get RID  
        """
        return self.rid

    def set_rid(self, new_rid:RID)->None:
        """
        Set RID  
        """
        self.rid = new_rid

    def get_columns(self)->list:
        """
        Get columns for Record
        """
        return self.columns

    def get_page_range_index(self)->int:
        """
        Get Page Range index for Record RID
        """
        return self.rid.get_page_range_index()

    def get_base_page_index(self)->int:
        """
        Get Base Page index for Record RID
        """
        return self.rid.get_base_page_index()
