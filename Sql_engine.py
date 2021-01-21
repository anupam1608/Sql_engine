import sqlparse
import csv
import operator
import os
import sys
from collections import defaultdict 

tables_to_col, table_data = defaultdict(list), defaultdict(list)
keywords, query_tables = [], []
operators = ["<=", ">=",">", "<", "="]
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
def print_table():
    pass


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

def is_where_present(idx, keywords):

    if len(keywords)>idx:
        if "WHERE" in keywords[idx]:
            return True
    return False

def is_int(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def validate_query(keywords):
    
    distinct_flag, where_flag = False, False
    if "DISTINCT" in keywords:
        distinct_flag = True
    if keywords[0] != "SELECT"   or "FROM" not in keywords:
        print("SELECT or FROM is missing")
        throw_error(er)
    if (distinct_flag == False and keywords[2] != "FROM") or distinct_flag and keywords[3] != "FROM" :
        print("No columns to project")
        throw_error(er)     
    
    if distinct_flag:
        query_tables, num_tables = get_query_tables(keywords[4])
        where_flag = is_where_present(5, keywords)        
            
    else:
        query_tables, num_tables = get_query_tables(keywords[3])
        where_flag = is_where_present(4, keywords)
    # print(where_flag)    
    

    return query_tables, num_tables, distinct_flag, where_flag   
    
def cartesian_product(query_tables):

    joined_table, joined_data = [], []
    for table in query_tables:
        for col in tables_to_col[table]:
            joined_table.append(col)
    for row1 in table_data[query_tables[0]]:
        for row2 in table_data[query_tables[1]]:
            joined_data.append(row1 + row2)
    
    return joined_table, joined_data

def extract_operator(left_condition, right_condition):

    operators_present = []
    
    for operator in operators:
        if operator in left_condition:
            operators_present.append(operator)
            break

    for operator in operators:
        if operator in right_condition:
            operators_present.append(operator)
            break

    # print("operators present", operators_present)

    return operators_present

def get_operands(joined_table, condition, operator_present):

    # print("In get_operands")
    operands_present = []
    if condition == "":
        return operands_present
    for operator in operator_present:
        if operator in condition:
            # print("operator ", operator)
            operands_present = condition.split(operator)
            operands_present = list(map(str.strip,operands_present))
            break
    # print(operands_present)

    if len(operands_present)<2:
        print("Operand missing in where clause")
        throw_error(er)
    
    return operands_present

def get_column_index(joined_table, operands_present):

    operands_idx = []
    value_flag = False
    for operand in operands_present:
        if is_int(operand) == False and operand in joined_table:
            operands_idx.append(joined_table.index(operand))
        elif is_int(operand):
            value_flag = True
            operands_idx.append(operand)
        else:
            print(f"{operand} Column not present")
            throw_error(er)
    # print("column idx", operands_idx)       
    return operands_idx, value_flag


def apply_where(joined_table, joined_data, operation_flag, operator_present, left_condition_operands, right_condition_operands):
    
    result = []
    ops = { "<": operator.lt, ">": operator.gt,"<=": operator.le, ">=": operator.ge,"=": operator.eq }
  
    for row in joined_data:

        if operation_flag == 0:
            left_condition_operands_idx, int_val_flag = get_column_index(joined_table, left_condition_operands)
            if int_val_flag:
                if ops[operator_present[0]](int(row[left_condition_operands_idx[0]]), int(left_condition_operands_idx[1])):
                    result.append(row)
            else:
                if ops[operator_present[0]](int(row[left_condition_operands_idx[0]]), int(row[left_condition_operands_idx[1]])):
                    result.append(row)
        else:
            left_condition_operands_idx, int_val_left_flag = get_column_index(joined_table, left_condition_operands)
            right_condition_operands_idx, int_val_right_flag = get_column_index(joined_table, right_condition_operands)
            
            if int_val_left_flag:
                left_condition_num = left_condition_operands_idx[1]
            else:
                left_condition_num = row[left_condition_operands_idx[1]]
            if int_val_right_flag:
                right_condition_num = right_condition_operands_idx[1]
            else:
                right_condition_num = row[right_condition_operands_idx[1]]
            
            if operation_flag == 1:
                if ops[operator_present[0]](int(row[left_condition_operands_idx[0]]), int(left_condition_num)) and ops[operator_present[1]](int(row[right_condition_operands_idx[0]]), int(right_condition_num)):
                    result.append(row)
            if operation_flag == 2:
                if ops[operator_present[0]](int(row[left_condition_operands_idx[0]]), int(left_condition_num)) or ops[operator_present[1]](int(row[right_condition_operands_idx[0]]), int(right_condition_num)):
                    result.append(row)

    # for row in result:
    #     print(row)

    return result

def handle_where_clause(joined_table, joined_data, distinct_flag, keywords):
    
    where_query = ""
    if distinct_flag:
        where_query = keywords[5][6:].strip()
    else:
        where_query = keywords[4][6:].strip()
    # print(where_query)
    if len(where_query.strip()) == 0:
        print("Error in where clause")
        throw_error(er)
    operation_flag = 0 # 0 - for no relational operation, 1 - for and operation, 2 - for or operation
    left_condition, right_condition = "", ""
    conditions = where_query.split()
    if "AND" in conditions:
        operation_flag = 1
        try:
            left_condition = where_query.split("AND")[0].strip()
            right_condition = where_query.split("AND")[1].strip()
        except:
            throw_error(er)

    if "OR" in conditions:
        operation_flag = 2
        try:
            left_condition = where_query.split("OR")[0].strip()
            right_condition = where_query.split("OR")[1].strip()
        except:
            throw_error(er)
        
    if operation_flag == 0:
        left_condition = where_query.strip()
    # print("left", left_condition)
    # print("right", right_condition)
    operator_present = extract_operator(left_condition, right_condition)    
    left_condition_operands = get_operands(joined_table, left_condition, operator_present)
    right_condition_operands = get_operands(joined_table, right_condition, operator_present)
    joined_data = apply_where(joined_table, joined_data, operation_flag, operator_present, left_condition_operands, right_condition_operands)

    return joined_data       
    
    

def handle_query():
    
    keywords =  parse_query(sys.argv[1])
    query_tables,num_tables, distinct_flag, where_flag =  validate_query(keywords)    
    if num_tables > 1:
        joined_table, joined_data = cartesian_product(query_tables)
    else:
        joined_table, joined_data = tables_to_col[query_tables[0]], table_data[query_tables[0]]

    if where_flag:
        joined_data = handle_where_clause(joined_table, joined_data, distinct_flag, keywords)

    if "Group BY" in keywords:
        handle_groupBy(joined_table, joined_data, keywords)    
    
    
    
    
        

def start():

    get_data()
    handle_query()
    

start()
### handle - where, aggregate, group by, order by, project 

