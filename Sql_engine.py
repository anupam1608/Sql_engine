import sqlparse
import csv
import os
import sys
from collections import defaultdict 

tables_to_col, table_data = defaultdict(list), defaultdict(list)
keywords = []
num_tables = 0
metafile = "metadata.txt"
m = "meta"
f = "file"
er = "invalid"
ext = ".csv"

def throw_error(type):

    if type == "meta":
        print("Error in meta-data file")
         
    elif type == "file":
        print("Error in data file")
        
    elif type == "invalid":
        print("Invalid query, Please check")

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
    # print(tables_to_col)
    read_data_files()
    # print(table_data)

def check_semicolon(query):

    if ";" not in query:
        return False
    return True

def parse_query(query):

    query = sqlparse.format(query,keyword_case = 'upper')
    if check_semicolon(query) == False:
        print("Semi-colon is missing in query")
        sys.exit()
    query = query.strip(";")
    # print(query)
    parsed_query = sqlparse.parse(query)[0].tokens
    # print(parsed_query)
    ids = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
    token_list = [str(id) for id in ids] 
    print(token_list)  
    return token_list

def validate_query(keywords):

    if "SELECT" not in keywords or "FROM" not in keywords:
        print("SELECT or FROM is missing")
        throw_error(er)
    if keywords[2] != "FROM":
        print("No columns to project")
        throw_error(er)
    query_tables = keywords[3]
    # print(query_tables)
    query_tables = query_tables.split(",")
    query_tables = [table.strip() for table in query_tables]
    num_tables = len(query_tables)
    for table in query_tables:
        if table not in tables_to_col:
            print(f"Unknown {table} is given in query")
            throw_error(er)
    
    
def handle_query():
    keywords =  parse_query(sys.argv[1])
    validate_query(keywords)   
def start():

    get_data()
    handle_query()
    

start()

