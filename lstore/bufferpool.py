import lstore.config as Config
from lstore.record import Record, RID
from lstore.frame import Frame
from lstore.disk import DISK


class Bufferpool:
    def __init__(self):
        self.frames: dict[str, Frame] = {}  # {page_path: Frame}

    def __has_capacity(self) -> bool:
        return len(self.frames) < Config.BUFFERPOOL_FRAME_SIZE

    def __is_record_in_buffer(self, page_path: str) -> bool:
        return page_path in self.frames

    def __evict_frame(self) -> None:
        # print("evicting frame")
        lru_time_frame = None
        lru_frame_path = ""

        # Find the least recently used frame that is not pinned
        for frame_path, frame_obj in self.frames.items():
            if not frame_obj.is_pin:
                if (
                    lru_time_frame is None
                    or frame_obj.last_time_used < lru_time_frame.last_time_used
                ):
                    lru_time_frame = frame_obj
                    lru_frame_path = frame_path

        # if lru_time_frame is not None:
        #     print(f"The frame with the highest last_time_used and not pinned is Frame {lru_time_frame} with frame index : {lru_frame_path} and path to page {lru_frame_path}")
        # else:
        #     print("No frame is currently not pinned.")

        # IF THE FRAME IS DIRTY, IT WRITES IT TO THE DISK
        if lru_frame_path and self.frames[lru_frame_path].is_dirty:

            # path to page range to write to
            path_to_physical_page = lru_frame_path

            i = 0
            for physical_page in self.frames[lru_frame_path].physical_pages:
                DISK.write_physical_page_to_disk(
                    path_to_physical_page=path_to_physical_page,
                    physical_page=physical_page,
                    page_index=i,
                )
                i += 1

            self.frames[lru_frame_path].set_clean()

        # Remove the frame from the buffer pool and directory
        if lru_frame_path:
            del self.frames[lru_frame_path]

        # print(f"Finished evicting {lru_frame_path}, new frame count {len(self.frames)}")

    def __import_frame(self, path_to_page: str, num_columns: int) -> None:
        if not self.__has_capacity():
            self.__evict_frame()

        self.frames[path_to_page] = Frame(path_to_page=path_to_page)
        self.frames[path_to_page].load_data(
            num_columns=num_columns, path_to_page=path_to_page
        )

    def insert_record(
        self,
        page_path: str,
        record: Record,
        num_columns=int,
        record_meta_data: list = None,
    ) -> None:
        if not self.__is_record_in_buffer(page_path):
            self.__import_frame(path_to_page=page_path, num_columns=num_columns)

        # if record meta_data is not none then it's tail page and it needs the meta data list
        if record_meta_data == None:
            self.frames[page_path].insert_record(record=record)
        else:
            self.frames[page_path].insert_record(
                record=record, record_meta_data=record_meta_data
            )

    def get_data_from_buffer(
        self, rid: RID, page_path: str, num_columns: int
    ) -> tuple:  # return data
        if not self.__is_record_in_buffer(page_path):
            self.__import_frame(path_to_page=page_path, num_columns=num_columns)

        return self.frames[page_path].get_data(rid)

    def get_meta_data(self, rid: RID, path_to_page: str, num_columns: int) -> list[int]:
        if not self.__is_record_in_buffer(page_path=path_to_page):
            self.__import_frame(path_to_page=path_to_page, num_columns=num_columns)

        return self.frames[path_to_page].get_meta_data(rid=rid)

    def update_meta_data(
        self, rid: RID, path_to_page: str, num_columns: int, meta_data: list
    ) -> None:
        if not self.__is_record_in_buffer(page_path=path_to_page):
            self.__import_frame(path_to_page=path_to_page, num_columns=num_columns)

        self.frames[path_to_page].update_meta_data(rid=rid, meta_data=meta_data)

    def delete_record(self, rid: RID, page_path: str, num_columns: int) -> None:
        """delete record from db in bufferpool."""
        if not self.__is_record_in_buffer(page_path=page_path):
            self.__import_frame(path_to_page=page_path, num_columns=num_columns)

        self.frames[page_path].delete_record(rid=rid)


BUFFERPOOL = Bufferpool()
