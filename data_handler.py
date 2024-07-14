import json

import numpy as np
from util_classes.PolarsFileSystem import PolarsFileSystem
from utils import generate_random_id
from pytz import timezone
from datetime import datetime
import polars as pl
from database_interface import DatabaseInterface

DB_NAME = "database_data.db"

class EmployeeData:

    def __init__(self) -> None:
        """

        """
        self.table_name = "employees"
        self.db = DatabaseInterface(DB_NAME)
        self.check_table_exists()
    
    def check_table_exists(self):
        query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT,
                created_at TEXT,
                phone_no TEXT,
                address TEXT,
                designation TEXT,
                description TEXT
                );
        """
        indexquery = f"""
        CREATE INDEX IF NOT EXISTS idx_employee_id ON {self.table_name}(employee_id);
        """
        self.db.execute_with_auto_commit(query)
        self.db.execute_with_auto_commit(indexquery)

        
    
    def add_employee(self, employee: dict):

        get_employee_full_name_query = f"SELECT full_name FROM {self.table_name} WHERE full_name = '{employee.get('full_name')}'"
        res = self.db.execute_select_query(get_employee_full_name_query)
        if not res.is_empty():
            raise ValueError(f"Employee with name {employee.get('full_name')} already exists")

        add_employee_query = f"""
        INSERT INTO {self.table_name} (full_name, created_at, phone_no, address, designation, description)
        VALUES (
            '{employee.get('full_name')}',
            '{datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:00')}',
            '{employee.get('phone_no')}',
            '{employee.get('address')}',
            '{employee.get('designation')}',
            '{employee.get('description')}'
        );
        """
        self.db.execute_with_auto_commit(add_employee_query)

    

    
    def get_employee(self, id: int):
        query = f"SELECT * FROM {self.table_name} WHERE employee_id = {id}"
        res = self.db.execute_select_query(query)
        if len(res) == 0:
            return None
        return res.to_dicts()[0]
    
    def get_all_employees(self):
        
        query = f"SELECT * FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        return res.to_dicts()
    
    def delete_employee(self, employee_id: int):
        query = f"DELETE FROM {self.table_name} WHERE employee_id = {employee_id}"
        self.db.execute_with_auto_commit(query)

    def update_employee(self, employee_id: int, employee: dict):

        get_all_names_query = f"SELECT full_name FROM {self.table_name} WHERE full_name = '{employee.get('full_name')}' and employee_id != {employee_id}"
        res = self.db.execute_select_query(get_all_names_query)
        if not res.is_empty():
            raise ValueError(f"Employee with name {employee.get('full_name')} already exists")

        query = f"""
        UPDATE {self.table_name}
        SET 
        full_name = '{employee.get('full_name')}',
        phone_no = '{employee.get('phone_no')}',
        address = '{employee.get('address')}',
        designation = '{employee.get('designation')}',
        description = '{employee.get('description')}'
        WHERE employee_id = {employee_id}
        """
        self.db.execute_with_auto_commit(query)
    
    def check_employee_exists(self, employee_id: int, company):
        query = f"SELECT * FROM {self.table_name} WHERE employee_id = {employee_id}"
        res = self.db.execute_select_query(query)
        return not res.is_empty()


class OwnCompanyData:

    def __init__(self) -> None:
        """

        """
        self.table_name = "own_companies"
        self.db = DatabaseInterface(DB_NAME)
        self.check_table_exists()

    def check_table_exists(self):
        query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                company_id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT,
                created_at TEXT,
                phone_no TEXT,
                address TEXT,
                alternate_phone_no TEXT,
                mail_id TEXT,
                type_of_company TEXT,
                gst_no TEXT,
                pan_no TEXT,
                bank_name TEXT,
                bank_branch TEXT,
                bank_ifsc_code TEXT,
                account_no TEXT,
                account_owner_name TEXT,
                date_of_establishment TEXT,
                description TEXT                
                );
        """
        indexquery = f"""
        CREATE INDEX IF NOT EXISTS idx_company_name ON {self.table_name}(company_name);
        """
        self.db.execute_with_auto_commit(query)
        self.db.execute_with_auto_commit(indexquery)
    
    def add_own_company(self, company: dict):
            
        get_company_name_query = f"SELECT company_name FROM {self.table_name} WHERE company_name = '{company.get('company_name')}'"
        res = self.db.execute_select_query(get_company_name_query)
        if not res.is_empty():
            raise ValueError(f"Company with name {company.get('company_name')} already exists")

        bank_name = company.get("bank_name")
        bank_branch = company.get("bank_branch")
        bank_ifsc_code = company.get("bank_ifsc_code")
        account_no = company.get("account_no")
        account_owner_name = company.get("account_owner_name")

        bank_name = json.dumps(bank_name)
        bank_branch = json.dumps(bank_branch)
        bank_ifsc_code = json.dumps(bank_ifsc_code)
        account_no = json.dumps(account_no)
        account_owner_name = json.dumps(account_owner_name)
        
        add_company_query = f"""
        INSERT INTO {self.table_name} (company_name, created_at, phone_no, address, alternate_phone_no, mail_id, type_of_company, gst_no, pan_no, bank_name, bank_branch, bank_ifsc_code, account_no, account_owner_name, date_of_establishment, description)
        VALUES (
            '{company.get('company_name')}',
            '{datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:00')}',
            '{company.get('phone_no')}',
            '{company.get('address')}',
            '{company.get('alternate_phone_no')}',
            '{company.get('mail_id')}',
            '{company.get('type_of_company')}',
            '{company.get('gst_no')}',
            '{company.get('pan_no')}',
            '{bank_name}',
            '{bank_branch}',
            '{bank_ifsc_code}',
            '{account_no}',
            '{account_owner_name}',
            '{company.get('date_of_establishment')}',
            '{company.get('description')}'
        );
        """
        self.db.execute_with_auto_commit(add_company_query)

    def get_all_own_companies(self):
        query = f"SELECT * FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        res = res.with_columns(bank_name=pl.col("bank_name").str.json_decode(pl.List(pl.Utf8)))
        res = res.with_columns(bank_branch=pl.col("bank_branch").str.json_decode(pl.List(pl.Utf8)))
        res = res.with_columns(bank_ifsc_code=pl.col("bank_ifsc_code").str.json_decode(pl.List(pl.Utf8)))
        res = res.with_columns(account_no=pl.col("account_no").str.json_decode(pl.List(pl.Utf8)))
        res = res.with_columns(account_owner_name=pl.col("account_owner_name").str.json_decode(pl.List(pl.Utf8)))
        return res.to_dicts()

    def get_all_own_company_names(self):
        query = f"SELECT company_name FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        res = res['company_name'].to_list()
        return res

    def update_own_company(self, id, company: dict):
        query_check_company_exists = f"SELECT * FROM {self.table_name} WHERE company_id = {id}"
        res = self.db.execute_select_query(query_check_company_exists)
        if res.is_empty():
            raise ValueError(f"Company does not exist")
        
        update_company_query = f"""
        UPDATE {self.table_name} 
        SET 
        company_name = '{company.get('company_name')}',
        phone_no = '{company.get('phone_no')}',
        address = '{company.get('address')}',
        alternate_phone_no = '{company.get('alternate_phone_no')}',
        mail_id = '{company.get('mail_id')}',
        type_of_company = '{company.get('type_of_company')}',
        gst_no = '{company.get('gst_no')}',
        pan_no = '{company.get('pan_no')}',
        bank_name = '{json.dumps(company.get('bank_name'))}',
        bank_branch = '{json.dumps(company.get('bank_branch'))}',
        bank_ifsc_code = '{json.dumps(company.get('bank_ifsc_code'))}',
        account_no = '{json.dumps(company.get('account_no'))}',
        account_owner_name = '{json.dumps(company.get('account_owner_name'))}',
        date_of_establishment = '{company.get('date_of_establishment')}',
        description = '{company.get('description')}'
        WHERE company_id = {id}
        """
        self.db.execute_with_auto_commit(update_company_query)

    def delete_own_company(self, id):
        check_company_exists_query = f"SELECT * FROM {self.table_name} WHERE company_id = {id}"
        res = self.db.execute_select_query(check_company_exists_query)
        if res.is_empty():
            raise ValueError(f"Company does not exist")

        query = f"DELETE FROM {self.table_name} WHERE company_id = {id}"
        self.db.execute_with_auto_commit(query)
    
    def check_own_company_exists(self, company_name):
        query = f"SELECT * FROM {self.table_name} WHERE company_name = '{company_name}'"
        res = self.db.execute_select_query(query)
        return not res.is_empty()




class SalaryData:

    def __init__(self) -> None:
        """
        
        """
        self.table_name = "salaries"
        self.db = DatabaseInterface(DB_NAME)
        self.check_table_exists()
    
    def check_table_exists(self):
        query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                salary_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                payment INTEGER,
                record_date TEXT,
                employee_id INTEGER,
                type_of_payment TEXT,
                mode_of_payment TEXT,
                company TEXT,
                work_ids TEXT,
                costs TEXT,
                quantities TEXT,
                work_done INTEGER,
                created_at TEXT
                );
        """
        indexquery = f"""
        CREATE INDEX IF NOT EXISTS idx_salary_entry_id ON {self.table_name}(salary_entry_id);
        """
        self.db.execute_with_auto_commit(query)
        self.db.execute_with_auto_commit(indexquery)

    def add_salary_entry(self, entry: dict):

        company = entry.get("company")
        own_company_data = OwnCompanyData()
        if not own_company_data.check_own_company_exists(company):
            raise ValueError(f"Company with name {company} does not exist")
        employee_data = EmployeeData()
        employee_id = entry.get("employee_id")
        if not employee_data.check_employee_exists(employee_id, company):
            raise ValueError(f"Employee with id {employee_id} does not exist")
        
        work_ids = entry.get("work_ids")
        costs = entry.get("costs")
        quantities = entry.get("quantities")

        dot_product = np.dot(costs, quantities)
        if entry.get('type_of_payment') == "advance":
            if dot_product > 0:
                raise ValueError(f"Advance payment not allowed in same entry with work done")

        work_ids = json.dumps(work_ids)
        costs = json.dumps(costs)
        quantities = json.dumps(quantities)

        salary_entry_query = f"""
        INSERT INTO {self.table_name} (payment, record_date, employee_id, type_of_payment, mode_of_payment, company, works, costs, quantities, work_done, created_at)
        VALUES (
            {entry.get('payment')},
            '{entry.get('record_date')}',
            {employee_id},
            '{entry.get('type_of_payment')}',
            '{entry.get('mode_of_payment')}',
            '{company}',
            '{work_ids}',
            '{costs}',
            '{quantities}',
            {dot_product},
            '{datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:00')}'
        );
        """
        self.db.execute_with_auto_commit(salary_entry_query)
        
    
    def get_all_salary_entries_of_an_employee(self, employee_id):
        query = f"SELECT * FROM {self.table_name} WHERE employee_id = {employee_id}"
        res = self.db.execute_select_query(query)
        res = res.with_columns(costs=pl.col("costs").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(quantities=pl.col("quantities").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(works=pl.col("work_ids").str.json_decode(pl.List(pl.Int64)))
        return res.to_dicts()
    
    def get_all_salary_entries(self):
        query = f"SELECT * FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        res = res.with_columns(costs=pl.col("costs").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(quantities=pl.col("quantities").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(work_ids=pl.col("work_ids").str.json_decode(pl.List(pl.Int64)))
        res = res.with_row_count()
        df = res.select(pl.col('work_ids'), pl.col('index'))
        df = df.explode(pl.col('work_ids'))

        work_handler = Works()
        works = work_handler.get_all_works_brief_as_df()

        df = df.join(works, left_on='work_ids', right_on='work_id').group_by('index').agg(pl.col('work_name'), pl.col('bus_type'))
        res = res.join(df, left_on='index', right_on='index')
        return res.to_dicts()
    
    def delete_salary_entry(self, employee_id, salary_entry_id):
        query = f"DELETE FROM {self.table_name} WHERE salary_entry_id = {salary_entry_id} AND employee_id = {employee_id}"
        self.db.execute_with_auto_commit(query)
    
    def update_salary_entry(self, salary_entry_id, entry: dict):
        query_check_salary_entry_exists = f"SELECT * FROM {self.table_name} WHERE salary_entry_id = {salary_entry_id}"
        res = self.db.execute_select_query(query_check_salary_entry_exists)
        if res.is_empty():
            raise ValueError(f"Salary entry with id {salary_entry_id} does not exist")

        # check own company exists
        company = entry.get("company")
        own_company_data = OwnCompanyData()
        if not own_company_data.check_own_company_exists(company):
            raise ValueError(f"Company with name {company} does not exist")
        
        # check employee exists
        employee_data = EmployeeData()
        employee_id = entry.get("employee_id")
        if not employee_data.check_employee_exists(employee_id, company):
            raise ValueError(f"Employee with id {employee_id} does not exist")
        
        works = entry.get("works")
        costs = entry.get("costs")
        quantities = entry.get("quantities")

        dot_product = np.dot(costs, quantities)

        works = json.dumps(works)
        costs = json.dumps(costs)
        quantities = json.dumps(quantities)

        update_salary_entry_query = f"""
        UPDATE {self.table_name}
        SET 
        payment = {entry.get('payment')},
        record_date = '{entry.get('record_date')}',
        type_of_work = '{entry.get('type_of_work')}',
        type_of_payment = '{entry.get('type_of_payment')}',
        mode_of_payment = '{entry.get('mode_of_payment')}',
        company = '{company}',
        works = '{works}',
        costs = '{costs}',
        quantities = '{quantities}',
        work_done = {dot_product}
        WHERE salary_entry_id = {salary_entry_id}
        """
        self.db.execute_with_auto_commit(update_salary_entry_query)
    
    def get_all_salary_entries_company(self, company):
        query = f"SELECT * FROM {self.table_name} WHERE company = '{company}'"
        res = self.db.execute_select_query(query)
        res = res.with_columns(costs=pl.col("costs").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(quantities=pl.col("quantities").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(works=pl.col("works").str.json_decode(pl.List(pl.Utf8)))
        return res.to_dicts()
    
    def get_all_salary_entries_of_an_employee_company(self, employee_id, company):
        query = f"SELECT * FROM {self.table_name} WHERE employee_id = {employee_id} AND company = '{company}'"
        res = self.db.execute_select_query(query)
        res = res.with_columns(costs=pl.col("costs").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(quantities=pl.col("quantities").str.json_decode(pl.List(pl.Int64)))
        res = res.with_columns(works=pl.col("works").str.json_decode(pl.List(pl.Utf8)))
        return res.to_dicts()
    


    

class SummaryInsights:

    def __init__(self) -> None:
        self.employees_table_name = "employees"

    def get_payment_summary(self, company):
        
        aggregate_query = f"""
        SELECT SUM(payment) as total_payment, AVG(payment) as average_payment, MAX(payment) as max_payment, MIN(payment) as min_payment,
        COUNT(payment) as total_entries, SUM(work_done) as total_work_done, AVG(work_done) as average_work_done, MAX(work_done) as max_work_done, MIN(work_done) as min_work_done
        FROM salaries WHERE company = '{company}';
        """
        db = DatabaseInterface(DB_NAME)
        res = db.execute_select_query(aggregate_query)
        if res.is_empty():
            return None
        return res.to_dicts()[0]
    

class Works:

    def __init__(self) -> None:
        self.table_name = "works"
        self.db = DatabaseInterface(DB_NAME)
        self.check_table_exists()
    
    def check_table_exists(self):
        query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                work_id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_name TEXT,
                bus_type TEXT,
                cost INTEGER,
                description TEXT
                );
        """
        indexquery = f"""
        CREATE INDEX IF NOT EXISTS idx_work_id ON {self.table_name}(work_id);
        """
        self.db.execute_with_auto_commit(query)
        self.db.execute_with_auto_commit(indexquery)
    
    def add_work(self, work: dict):
        get_work_name_query = f"SELECT work_name FROM {self.table_name} WHERE work_name = '{work.get('work_name')}'"
        res = self.db.execute_select_query(get_work_name_query)
        if not res.is_empty():
            raise ValueError(f"Work with name {work.get('work_name')} already exists")
        
        bus_types = BusTypes()
        if not bus_types.check_bus_type_exists(work.get('bus_type')):
            raise ValueError(f"Bus type with name {work.get('bus_type')} does not exist")
        
        add_work_query = f"""
        INSERT INTO {self.table_name} (work_name, bus_type, cost, description)
        VALUES (
            '{work.get('work_name')}',
            '{work.get('bus_type')}',
            {work.get('cost')},
            '{work.get('description')}'
        );
        """
        self.db.execute_with_auto_commit(add_work_query)
    
    def get_all_works_brief(self):
        query = f"SELECT work_id, work_name, bus_type, cost FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        return res.to_dicts()
    
    def get_all_works_brief_as_df(self):
        query = f"SELECT work_id, work_name, bus_type, cost FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        return res

    def delete_work(self, work_id):
        # check if work exists
        query = f"SELECT * FROM {self.table_name} WHERE work_id = {work_id}"
        res = self.db.execute_select_query(query)
        if res.is_empty():
            raise ValueError(f"Work with id {work_id} does not exist")

        query = f"DELETE FROM {self.table_name} WHERE work_id = {work_id}"
        self.db.execute_with_auto_commit(query)
        return True
    
    def update_work(self, work_id, work: dict):
        query_check_work_exists = f"SELECT * FROM {self.table_name} WHERE work_id = {work_id}"
        res = self.db.execute_select_query(query_check_work_exists)
        if res.is_empty():
            raise ValueError(f"Work with id {work_id} does not exist")
        
        query = f"""
        UPDATE {self.table_name}
        SET 
        work_name = '{work.get('work_name')}',
        bus_type = '{work.get('bus_type')}',
        cost = {work.get('cost')},
        description = '{work.get('description')}'
        WHERE work_id = {work_id}
        """
        self.db.execute_with_auto_commit(query)


class BusTypes:

    def __init__(self) -> None:
        self.table_name = "bus_types"
        self.db = DatabaseInterface(DB_NAME)
        self.check_table_exists()

    def check_table_exists(self):
        query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                bus_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                bus_type TEXT
                );
        """
        indexquery = f"""
        CREATE INDEX IF NOT EXISTS idx_bus_type_id ON {self.table_name}(bus_type_id);
        """
        self.db.execute_with_auto_commit(query)
        self.db.execute_with_auto_commit(indexquery)

    def add_bus_type(self, bus_type: dict):
        get_bus_type_query = f"SELECT bus_type FROM {self.table_name} WHERE bus_type = '{bus_type.get('bus_type')}'"
        res = self.db.execute_select_query(get_bus_type_query)
        if not res.is_empty():
            raise ValueError(f"Bus type with name {bus_type.get('bus_type')} already exists")
        
        add_bus_type_query = f"""
        INSERT INTO {self.table_name} (bus_type)
        VALUES (
            '{bus_type.get('bus_type')}'
        );
        """
        self.db.execute_with_auto_commit(add_bus_type_query)

    def get_all_bus_types(self):
        query = f"SELECT * FROM {self.table_name}"
        res = self.db.execute_select_query(query)
        return res
    
    def delete_bus_type(self, bus_type_id):
        # check if the bus type exists
        query = f"SELECT * FROM {self.table_name} WHERE bus_type_id = {bus_type_id}"
        res = self.db.execute_select_query(query)
        if res.is_empty():
            raise ValueError(f"Bus type with id {bus_type_id} does not exist")
        
        query = f"DELETE FROM {self.table_name} WHERE bus_type_id = {bus_type_id}"
        self.db.execute_with_auto_commit(query)

    def update_bus_type(self, bus_type_id, bus_type: dict):
        query_check_bus_type_exists = f"SELECT * FROM {self.table_name} WHERE bus_type_id = {bus_type_id}"
        res = self.db.execute_select_query(query_check_bus_type_exists)
        if res.is_empty():
            raise ValueError(f"Bus type with id {bus_type_id} does not exist")
        
        query = f"""
        UPDATE {self.table_name}
        SET 
        bus_type = '{bus_type.get('bus_type')}'
        WHERE bus_type_id = {bus_type_id}
        """
        self.db.execute_with_auto_commit(query)
    
    def check_bus_type_exists(self, bus_type):
        query = f"SELECT * FROM {self.table_name} WHERE bus_type = '{bus_type}'"
        res = self.db.execute_select_query(query)
        return not res.is_empty()

    