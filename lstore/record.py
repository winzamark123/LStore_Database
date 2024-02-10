class Record:

    def __init__(self, rid:int, key:int, columns:tuple)->None:
        self.rid = rid
        self.key = key
        self.columns = columns