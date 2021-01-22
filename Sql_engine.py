import sqlparse
import csv
import operator
import os
import sys
from collections import defaultdict 

tables_to_col, table_data = defaultdict(list), defaultdict(list)
keywords, query_tables = [], []
operators = ["<=", ">=",">", "<", "="]
aggregate_functions = ["SUM", "AVG", "MAX", "MIN", "COUNT"]
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

def get_operands(joined_table, condition, operator):

    print("In get_operands")
    print(operator)
    operands_present = []
    if condition == "":
        return operands_present
    
    if operator in condition:
        print("operator ", operator)
        operands_present = condition.split(operator)
        operands_present = list(map(str.strip,operands_present))
        
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
    left_condition_operands = get_operands(joined_table, left_condition, operator_present[0])
    right_condition_operands = get_operands(joined_table, right_condition, operator_present[1])
    print("left",left_condition_operands)
    print("right", right_condition_operands)
    joined_data = apply_where(joined_table, joined_data, operation_flag, operator_present, left_condition_operands, right_condition_operands)

    return joined_data       
    
def handle_groupBy(joined_table, joined_data, keywords):

    idx = keywords.index("GROUP BY")
    groupby_col = ""
    if len(keywords) == idx + 1:
        print("column missing in GROUP BY clause")
        throw_error(er)
    groupby_col = keywords[idx + 1]
    if len(keywords) > idx+2:
        throw_error(er)
    if groupby_col not in joined_table:
        print("Column not present given in group by clause")
        throw_error(er)
    group_set = set()
    groupby_col_idx = joined_table.index(groupby_col)

    for row in joined_data:
        group_set.add(row[groupby_col_idx])

    return group_set, groupby_col

def extract_cols_and_function(given_cols):

    given_cols = given_cols.split(",")
    cols_and_aggregate = defaultdict(str)
    aggregate_flag = False
    for val in given_cols:
        l = val.split("(")
        l = list(map(str.strip, l))
        if len(l)>2:
            throw_error(er)
        if len(l) == 1:
            cols_and_aggregate[l[0]] == None
        else:
            val = l[0].strip()
            cols_and_aggregate[l[1].split(")")[0].strip()] = val.upper()
            aggregate_flag = True

    return cols_and_aggregate, aggregate_flag

def is_valid(cols_with_aggregate, joined_table, groupby_col):

    for col in cols_with_aggregate:
        if col != "*" and col not in joined_table:
            print(f"{col} column not found")
            return False
        if cols_with_aggregate[col] != "" and cols_with_aggregate[col] not in aggregate_functions:
            print(f"{cols_with_aggregate[col] } Unknown function")
            return False
        if cols_with_aggregate[col] == "" and groupby_col != "" and col != groupby_col:
            print(f"{col} should be in aggregate function or with group by clause")
            return False
    
    return True
def apply_aggregate(joined_table, joined_data, col, function, groupby_col = "", group_identifier = 0):

    col_idx = joined_table.index(col)
    col_values = []
    for row in joined_data:
        if groupby_col == "":
            col_values.append(int(row[col_idx]))
        else:
            idx = joined_table.index(groupby_col)
            if row[idx] == group_identifier:
                col_values.append(int(row[col_idx]))
     
    if function == "SUM":
        return sum(col_values)
    elif function == "MIN":
        return min(col_values)
    elif function == "MAX":
        return max(col_values)
    elif function == "AVG":
        return sum(col_values)/len(col_values)
    elif function == "COUNT":
        return len(col_values)


def apply_select(joined_table, joined_data, cols_with_aggregate, aggregate_flag, groupby_col, group_set):
    # print(aggregate_flag, groupby_col, group_set)
    result_table, result_data = [], []
    if aggregate_flag == False:
        if groupby_col == "":
            if "*" in cols_with_aggregate and len(cols_with_aggregate) == 1:
                result_table, result_data = joined_table, joined_data
            elif "*" in cols_with_aggregate and len(cols_with_aggregate)>1:
                throw_error(er)
            else:
                for col in cols_with_aggregate:
                    result_table.append(col)
                for row in joined_data:
                    temp_list = []
                    for col in result_table:
                        idx = joined_table.index(col)
                        temp_list.append(row[idx])
                    result_data.append(temp_list)
        else:
            result_table.append(groupby_col)
            for val in group_set:
                temp_list = []
                temp_list.append(val)
                result_data.append(temp_list)
    
    else:
        if groupby_col == "":
            for col in cols_with_aggregate:
                if col == "*" and cols_with_aggregate[col] == "COUNT":
                    result_table.append("COUNT(*)")
                    result_data.append(len(joined_data))
                elif col == "*" and cols_with_aggregate[col] != "COUNT":
                    throw_error(er)
                elif cols_with_aggregate[col] == "":
                    throw_error(er)
                else:
                    result_table.append(cols_with_aggregate[col] + "(" + col + ")")
                    result_data.append(apply_aggregate(joined_table, joined_data, col, cols_with_aggregate[col]))
        else:
            result_table.append(groupby_col)
            for col in cols_with_aggregate:
                if col != groupby_col and cols_with_aggregate[col] == "" or col == "*":
                    throw_error(er)
                if col == groupby_col:
                    continue

                result_table.append(cols_with_aggregate[col] + "(" + col + ")")
                for group_identifier in group_set:
                    temp_list = []
                    temp_list.append(group_identifier) 
                    temp_list.append(apply_aggregate(joined_table, joined_data, col, cols_with_aggregate[col], groupby_col, group_identifier))     
                    result_data.append(temp_list)

    return result_table,result_data                
 
 
def handle_cols_to_project(joined_table, joined_data, keywords, distinct_flag, groupby_col, group_set):
    
    if distinct_flag:
        given_cols = keywords[2]
    else:
        given_cols = keywords[1]
    
    cols_with_aggregate, aggregate_flag = extract_cols_and_function(given_cols)
    # print(cols_with_aggregate)
    if is_valid(cols_with_aggregate, joined_table, groupby_col) == False:
        throw_error(er)
    print(joined_table)
    for row in joined_data:
        print(row)
    joined_table,  joined_data = apply_select(joined_table, joined_data, cols_with_aggregate, aggregate_flag, groupby_col, group_set)   
    print(joined_table)
    for row in joined_data:
        print(row)
    




def handle_query():
    
    keywords =  parse_query(sys.argv[1])
    query_tables,num_tables, distinct_flag, where_flag =  validate_query(keywords) 
    group_set, groupby_col = set(), ""   
    if num_tables > 1:
        joined_table, joined_data = cartesian_product(query_tables)
    else:
        joined_table, joined_data = tables_to_col[query_tables[0]], table_data[query_tables[0]]

    if where_flag:
        joined_data = handle_where_clause(joined_table, joined_data, distinct_flag, keywords)

    if "GROUP BY" in keywords:
        group_set, groupby_col = handle_groupBy(joined_table, joined_data, keywords) 
    
    handle_cols_to_project(joined_table, joined_data, keywords, distinct_flag, groupby_col, group_set)
      
    
        

def start():

    get_data()
    handle_query()
    
start()

