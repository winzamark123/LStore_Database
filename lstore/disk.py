import io
import os
from pickle import load, dump

class Disk:

  def create_path_directory(path:str)->None:
    if os.path.exists(path): raise FileExistsError
    os.mkdir(path)

  def list_directories_in_path(path:str)->list[str]:
    rlist = os.listdir(path)
    try: rlist.remove(".metadata.pkl")
    except ValueError: pass
    try: rlist.remove("index")
    except ValueError: pass
    rlist = [os.path.join(path, _) for _ in rlist]
    return rlist

  def write_to_path_metadata(path:str, metadata:dict)->None:
    with io.open(os.path.join(path, ".metadata.pkl"), 'wb') as f:
      dump(metadata, f)

  def read_from_path_metadata(path:str)->dict:
    with io.open(os.path.join(path, ".metadata.pkl"), 'rb') as f:
      return load(f)
