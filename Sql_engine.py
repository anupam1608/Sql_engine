import sqlparse
import csv
import os
import sys
from collections import defaultdict 

tables_to_col, table_data = defaultdict(list), defaultdict(list)
metafile = "metadata.txt"
m = "meta"
f = "file"
ext = ".csv"

def throw_error(type):

    if type == "meta":
        print("Error in meta-data file")
        sys.exit() 
    elif type == "file":
        print("Error in data file")
        sys.exit()

def read_meta_data():
    try:
        f = open(metafile, "r")
    except:
        throw_error(m)
    flag  = False
    name = ""    
    for line in f:
        line = line.strip()
        if line == "<begin_table>":
            flag = True
        elif flag:
            name = line
            flag = False
        elif line != "<end_table>":
            tables_to_col[name].append(line)
        else:
            pass   
    
            
        
def read_data_files():

    for file in tables_to_col:
        try:
            with open(file + ext, "r") as data_file:
                data = csv.reader(data_file)
                for row in data:
                    table_data[file].append(row)
        except:
            throw_error(f) 
              
def get_data():

    read_meta_data()
    print(tables_to_col)
    read_data_files()
    print(table_data)
    
## start engine   
def start():

    get_data()
    

start()

