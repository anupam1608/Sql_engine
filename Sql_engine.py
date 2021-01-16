import sqlparse
import csv
import os
import sys
from collections import defaultdict 

tables_to_col, table_data = defaultdict(list), defaultdict(list)
keywords, query_tables = [], []
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
def get_query_tables(tables):

    query_tables = tables.split(",")
    query_tables = [table.strip()for table in query_tables]
    # print(query_tables)
    num_tables = len(query_tables)
    for table in query_tables:
        if table not in tables_to_col:
            print(f"Unknown {table} is given in query")
            throw_error(er)

    return query_tables, num_tables

def validate_query(keywords):

    if keywords[0] != "SELECT"   or "FROM" not in keywords:
        print("SELECT or FROM is missing")
        throw_error(er)
    if keywords[2] != "FROM":
        print("No columns to project")
        throw_error(er)     
    
    query_tables = get_query_tables(keywords[3])
    return query_tables    
    
def cartesian_product(query_tables):

    joined_table, joined_data = [], []
    for table in query_tables:
        for col in tables_to_col[table]:
            joined_table.append(col)
    for row1 in table_data[query_tables[0]]:
        for row2 in table_data[query_tables[1]]:
            joined_data.append(row1 + row2)
    
    return joined_table, joined_data


def handle_query():
    keywords =  parse_query(sys.argv[1])
    query_tables,num_tables =  validate_query(keywords)
    print("query tables ", query_tables)
    print(num_tables)
    if num_tables > 1:
        joined_table, joined_data = cartesian_product(query_tables)
    else:
        joined_table, joined_data = tables_to_col[query_tables[0]], table_data[query_tables[0]]
    print(joined_table)
    for row in joined_data:
        print(row)
    
        

def start():

    get_data()
    handle_query()
    

start()

