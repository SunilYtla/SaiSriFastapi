import polars as pl
import pandas as pd
import featherstore as fs
from featherstore._table.read import convert_table_to_polars
from pytz import timezone
from datetime import datetime

# import sys
# import os
# current_directory = os.getcwd()
# sys.path.append(current_directory)

from utils import generate_random_id

EMPLOYEE_TABLE_NAME = "employees"
EMPLOYEE_SALARY_ENTRY_TABLE = "employee_salaries"
STORE = "feather_store"


def connect_to_fs_store(path: str, store_name: str):
    try:
        fs.create_database(path)
        fs.connect(path)
        if store_name not in fs.list_stores():
            fs.create_store(store_name)
        store = fs.Store(store_name)
        return store
    except Exception as e:
        print(str(e))
        fs.connect(path)
        if store_name not in fs.list_stores():
            fs.create_store(store_name)
        store = fs.Store(store_name)
        return store

def increment_char(c):
    """ Increment a character to the next in the alphabet. """
    if c == 'z':
        return 'a'
    return chr(ord(c) + 1)

def get_search_strings(s: str):
    last_char = s[-1]
    if last_char == 'z':
        if len(s) == 1:
            # Special case: if the string is just 'z', convert it to 'a'
            modified_string1 = 'a'
        else:
            # Increment the second-to-last character and set the last character to 'a'
            second_last_char = s[-2]
            incremented_char = increment_char(second_last_char)
            modified_string1 = s[:-2] + incremented_char + 'a'
    else:
        # Handle regular case where the last character is not 'z'
        next_char = chr(ord(last_char) + 1) if last_char != 'z' else 'z'

        modified_string1 = s[:-1] + next_char
    return s, modified_string1


def read_table_from_fs_store(store, table_name: str, columns: list = None, filter= None, before=None, after=None):
    if filter is not None:
        if before is not None:
            raise ValueError("Cannot use filter and before together")
        if after is not None:
            raise ValueError("Cannot use filter and after together")
    
    table = store.select_table(table_name)
    if table.exists():
        if filter is not None:
            filter_1, filter_2 = get_search_strings(filter)
            df = store.read_arrow(table_name, rows={'between': [filter_1, filter_2]}, cols=columns)
            if df.num_rows > 0:
                return convert_table_to_polars(df)
            else:
                return None
        
        if after is not None and before is None:
            df = store.read_polars(table_name, rows={'after': after}, cols=columns)
            return df
        
        if before is not None and after is None:
            df = store.read_polars(table_name, rows={'before': before}, cols=columns)
            return df
        
        if before is not None and after is not None:
            df = store.read_polars(table_name, rows={'between': [after, before]}, cols=columns)
            return df
        
        df = store.read_polars(table_name, cols=columns)
        return df
    else:
        return None

def write_table_to_fs_store(store, table_name: str, df: pl.DataFrame, create_new_table=False, index_col=None):

    df = df.to_pandas()
    if index_col is not None:
        df.set_index(index_col, inplace=True)

    table = store.select_table(table_name)
    if create_new_table:
        if table.exists() is False:
            table.write(df)
        return
    if table.exists():
        table.insert(df)
    else:
        table.write(df)
        



def disconnect_from_fs_store():
    try:
        fs.disconnect()
    except Exception as e:
        print(str(e))

class EmployeeHandler:

    def __init__(self):
        import os
        store_path = os.path.join(os.getcwd(), "store")
        self.store = connect_to_fs_store(store_path, STORE)

    def get_all_employees(self):
        df = read_table_from_fs_store(self.store, EMPLOYEE_TABLE_NAME,)
        return df.to_dicts()
    
    def get_employee(self, employee_id: str):
        search_index_term = employee_id.split("_")[0]
        df = read_table_from_fs_store(self.store, EMPLOYEE_SALARY_ENTRY_TABLE, filter=search_index_term)
        df = df.filter(pl.col("employee_id") == employee_id)
        return df.to_dicts()
    
    def add_employee(self, employee: dict):
        name_series = read_table_from_fs_store(self.store, EMPLOYEE_TABLE_NAME, columns=["name"])['name']
        df = pl.DataFrame(employee)
        current_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:00')
        df = df.with_columns(pl.lit(current_time).alias("created_at"))
        df = df.with_columns(pl.lit(generate_random_id(16)).alias("employee_id"))
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col("name"),
                    pl.col("employee_id")
                ],
                separator="_"
            ).alias("index")
        )
        df = df.with_columns(pl.col("index").alias("index_copy"))
        if name_series is None:
            write_table_to_fs_store(self.store, EMPLOYEE_TABLE_NAME, df, index_col="index")
            disconnect_from_fs_store()
            return
        name = employee.get("name")
        if name in name_series:
            disconnect_from_fs_store()
            raise ValueError(f"Employee with name {name} already exists")
        else:
            write_table_to_fs_store(self.store, EMPLOYEE_TABLE_NAME, df, index_col="index")
            disconnect_from_fs_store()
    
    def delete_employee(self, employee_id: int):
        name_series = read_table_from_fs_store(self.store, EMPLOYEE_TABLE_NAME, columns=["employee_id"])
        if name_series.list.contains(employee_id):
            df = read_table_from_fs_store(self.store, EMPLOYEE_TABLE_NAME, filter=employee_id)
            write_table_to_fs_store(self.store, EMPLOYEE_TABLE_NAME, df)
        else:
            raise ValueError(f"Employee with id {employee_id} does not exist")
    
    def update_employee(self, employee_id: int, employee: dict):
        df = read_table_from_fs_store(self.store, EMPLOYEE_TABLE_NAME, filter=employee_id)
        if df.shape[0] == 1:
            df = pl.DataFrame(employee)
            write_table_to_fs_store(self.store, EMPLOYEE_TABLE_NAME, df)
        else:
            raise ValueError(f"Employee with id {employee_id} does not exist")
    
    def create_employee_salary_entry(self, entry: dict, employee_id: str):
        name = employee_id.split("_")[0]
        df = pl.DataFrame(entry)
        df = df.with_columns(pl.lit(generate_random_id(16)).alias("salary_entry_id"))
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.lit(name),
                    pl.col("salary_entry_id")
                ],
                separator="_"
            ).alias("index")
        )
        df = df.with_columns(pl.col("index").alias("index_copy"))
        write_table_to_fs_store(self.store, EMPLOYEE_SALARY_ENTRY_TABLE, df, index_col="index")
        disconnect_from_fs_store()

    
    def get_employee_salary_entry(self, employee_id: str, salary_entry_id: str):
        df = read_table_from_fs_store(self.store, EMPLOYEE_SALARY_ENTRY_TABLE, filter=salary_entry_id)
        df = df.filter(pl.col("index") == salary_entry_id)
        return df.to_dicts()
    

