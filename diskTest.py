import os 
import json 

dir_path = os.getcwd() + '/ECS165/Grades'

files = os.listdir(dir_path)

for file in files:
        # Open the file
    with open(os.path.join(dir_path, file), 'rb') as f:
        # Read the file
        contents = f.read()
        # Print the contents
        print(contents)