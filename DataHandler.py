from util_classes.PolarsFileSystem import PolarsFileSystem
from utils import generate_random_id
from pytz import timezone
from datetime import datetime
import polars as pl

class EmployeeData:

    def __init__(self, store='employees') -> None:
        """
        we use same table for all employees and index on employee_id
        """
        self.store = store
        self.pfs = DataHandler()
        self.fs = self.pfs.get_fs_store()
    
    def add_employee(self, employee: dict, table):
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
            ).alias("employee_id")
        )
        # check if employee already exists
        name = employee.get("name")
        try:
            name_series = self.fs.read_dataframe(self.store, table)['name']
        except Exception as e:
            name_series = None
        if name_series is None or name not in name_series:
            self.fs.write_dataframe(df, self.store, table, 'employee_id', provided="table_df")
            return
        if name in name_series:
            raise ValueError(f"Employee with name {name} already exists")
    
    def check_employee_exists(self, employee_id, table):
        try:
            name_series = self.fs.read_dataframe(self.store, table)['employee_id']
        except Exception as e:
            name_series = None
        if name_series is None or employee_id not in name_series:
            return False
        return True
    
    def get_employee(self, employee_id: str, table):
        df = self.fs.read_dataframe(self.store, table, employee_id )
        df = df.filter(pl.col("employee_id") == employee_id)
        return df.to_dicts()
    
    def get_all_employees(self, table=None):
        if table:
            df = self.fs.read_dataframe(self.store, table)
        else:
            df = self.fs.read_entire_store(self.store)
        return df.to_dicts()
    
    def delete_employee(self, employee_id: str, table):
        self.fs.delete_index(self.store, table, employee_id)



class SalaryData:

    def __init__(self) -> None:
        """
        we need different tables for different employees and for same 
        employees we index on employee_id_salary_year_month
        """
        self.pfs = DataHandler()
        self.fs = self.pfs.get_fs_store()

    def add_salary_entry(self, entry: dict, employee_id: str, store, company: str):

        employee_data = EmployeeData()
        if not employee_data.check_employee_exists(employee_id, company):
            raise ValueError(f"Employee with id {employee_id} does not exist")

        df = pl.DataFrame(entry)
        current_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:00')
        yr_mnth = datetime.now(timezone("Asia/Kolkata")).strftime('%Y_%m')
        df = df.with_columns(pl.lit(current_time).alias("created_at"))
        df = df.with_columns(pl.lit(yr_mnth).alias("salary_year_month"))
        df = df.with_columns(pl.lit(generate_random_id(16)).alias("salary_entry_id"))
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col("employee_id"),
                    pl.col("salary_entry_id")
                ],
                separator="_"
            ).alias("salary_entry_id")
        )
        df = df.with_columns(pl.col('details').struct.field("costs"), pl.col('details').struct.field("quantities"))
        slice_size = 1000
        result = pl.concat([
            (
                df
                .slice(next_index, slice_size)
                .select(['costs', 'quantities'])
                .with_row_count()
                .explode(['costs', 'quantities'])
                .groupby('row_nr', maintain_order=True)
                .agg(pl.col('costs').dot('quantities').alias('amount_calculated'))
                .drop('row_nr')
            )
            for next_index in range(0, df.height, slice_size)
        ])
        df = df.hstack(result)
        df = df.drop(["costs", "quantities"])
        if self.fs.check_index_exists(store, employee_id, yr_mnth):
            _df = self.fs.read_dataframe(store, employee_id, yr_mnth)
            df = pl.concat([_df, df])
            self.fs.write_dataframe(df, store, employee_id, "salary_year_month", provided="table_df")
        else:
            self.fs.write_dataframe(df, store, employee_id, "salary_year_month", provided="table_df")
    
    def get_all_salary_entries(self, employee_id, store):
        df = self.fs.read_dataframe(store, employee_id)
        if df is None:
            return None
        return df.to_dicts()
    
    def get_salary_entry(self, employee_id, company, salary_year_month, salary_entry_id=None):
        df = self.fs.read_dataframe(company, employee_id, salary_year_month)
        if salary_entry_id:
            df = df.filter(pl.col("salary_entry_id") == salary_entry_id)
        return df.to_dicts()
    
    def get_all_employees_salary_entries(self, store):
        df = self.fs.read_entire_store(store)
        if df is None:
            return None
        return df.to_dicts()
    
    def delete_salary_entry(self, employee_id, company, salary_year_month, salary_entry_id):
        df = self.fs.read_dataframe(company, employee_id, salary_year_month)
        df = df.filter(pl.col("salary_entry_id") != salary_entry_id)
        self.fs.write_dataframe(df, company, employee_id, "salary_year_month", provided="table_df")
    
    def update_salary_entry(self, employee_id, company, salary_year_month, salary_entry_id, entry):
        df = self.fs.read_dataframe(company, employee_id, salary_year_month)
        df = df.filter(pl.col("salary_entry_id") != salary_entry_id)
        df = pl.concat([df, pl.DataFrame(entry)])
        self.fs.write_dataframe(df, company, employee_id, "salary_year_month", provided="table_df")

    


class DataHandler:

    def __init__(self) -> None:
        self.path = r"C:\Users\sunil\PythonProjects\FastApi\PolarsData"
        self.fs = PolarsFileSystem(self.path)
    
    def get_fs_store(self):
        return self.fs
    