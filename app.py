from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from employees.employee import EmployeeHandler
from data_handler import EmployeeData, SalaryData, OwnCompanyData, SummaryInsights, Works
import logging
import traceback




logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)  # Set logging level as needed

import polars as pl

app = FastAPI()

origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/get_all_employees")
async def get_all_employees(key: str | None = None, token: str| None = None):
    try:
        employee_handler = EmployeeData()
        employees = employee_handler.get_all_employees()
        return {"employees": employees, "has_error": False}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}


@app.get("/get_employee")
async def get_employee(employee_id: int | None = None, key: str | None = None, token: str| None = None):
    try:
        employee_handler = EmployeeData()
        employee = employee_handler.get_employee(employee_id)
        return employee
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    

@app.post("/add_employee")
async def add_employee(request: dict | None=None, key: str | None = None, token: str| None = None):
    try:
        employee_handler = EmployeeData()
        employee_handler.add_employee(request['data'])
        return {"msg": "Employee added successfully", "has_error": False}
    except Exception :
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    

@app.delete("/delete_employee")
async def delete_employee(employee_id: int, key: str | None = None, token: str| None = None):
    try:
        employee_handler = EmployeeData()
        employee_handler.delete_employee(employee_id)
        return {"msg": "Employee deleted successfully", "has_error": False}
    except Exception :
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

@app.put("/update_employee")
async def update_employee(employee_id: str, request: dict | None=None, key: str | None = None, token: str| None = None):
    try:
        employee_handler = EmployeeData()
        employee_handler.update_employee(employee_id, request['data'])
        return {"msg": "updated sucessfulley", "has_error": False}
    except Exception :
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

@app.post("/create_own_company")
async def create_own_company(payload: dict, key: str | None = None, token: str| None = None):
    try:
        own_company_handler = OwnCompanyData()
        own_company_handler.add_own_company(payload['data'])
        return {"company": payload, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

@app.get("/get_all_own_companies")
async def get_all_own_companies(key: str | None = None, token: str| None = None):
    try:
        own_company_handler = OwnCompanyData()
        companies = own_company_handler.get_all_own_companies()
        return {"companies": companies, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    
@app.get("/get_all_own_company_names")
async def get_all_own_company_names(key: str | None = None, token: str| None = None):
    try:
        own_company_handler = OwnCompanyData()
        companies = own_company_handler.get_all_own_company_names()
        return {"companies": companies, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    
@app.put("/update_own_company")
async def update_own_company(company_id: int, payload: dict, key: str | None = None, token: str| None = None):
    try:
        own_company_handler = OwnCompanyData()
        own_company_handler.update_own_company(company_id, payload['data'])
        return {"company_id": company_id, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    
@app.delete("/delete_own_company")
async def delete_own_company(company_id: int, key: str | None = None, token: str| None = None):
    try:
        own_company_handler = OwnCompanyData()
        own_company_handler.delete_own_company(company_id)
        return {"company_id": company_id, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

    

@app.post("/create_employee_salary_entry")
async def create_employee_salary_entry(payload: dict|None=None, key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_handler.add_salary_entry(payload['data'])
        return {"employee_id": payload['data']['employee_id'], "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    
@app.get("/get_all_salary_entries")
async def get_all_salary_entries(key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_entries = salary_handler.get_all_salary_entries()
        return {"salary_entries": salary_entries, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    

@app.get("/get_all_salary_entries_company")
async def get_all_salary_entries_company(company: str, key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_entries = salary_handler.get_all_salary_entries_company(company)
        return {"salary_entries": salary_entries, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

@app.get("/get_employee_salary_entries")
async def get_employee_salary_entries(employee_id: int, key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_entries = salary_handler.get_all_salary_entries_of_an_employee(employee_id)
        if salary_entries is None:
            return {"error": "No salary entries found", "has_error": True}
        return {"salary_entries": salary_entries, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

@app.get("/get_employee_salary_entries_company")
async def get_employee_salary_entries_company(employee_id: int, company: str, key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_entries = salary_handler.get_all_salary_entries_of_an_employee_company(employee_id, company)
        if salary_entries is None:
            return {"error": "No salary entries found", "has_error": True}
        return {"salary_entries": salary_entries, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

    

@app.delete("/delete_employee_salary_entry")
async def delete_employee_salary_entry(employee_id: str, salary_entry_id: str, key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_handler.delete_salary_entry(employee_id, salary_entry_id)
        return {"employee_id": employee_id, "salary_entry_id": salary_entry_id, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    

@app.put("/update_employee_salary_entry")
async def update_employee_salary_entry(payload: dict, key: str | None = None, token: str| None = None):
    try:
        salary_handler = SalaryData()
        salary_handler.update_salary_entry(payload['data']['salary_entry_id'], payload['data'])
        return {"employee_id": payload['data']['employee_id'], "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}


@app.get("/company_payment_summary")
async def company_payment_summary(company: str, key: str | None = None, token: str| None = None):
    try:
        summary_handler = SummaryInsights()
        summary = summary_handler.get_payment_summary(company)
        return {"summary": summary, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}



@app.get("/get_all_works")
async def get_all_works(key: str | None = None, token: str| None = None):
    try:
        works_handler = Works()
        works = works_handler.get_all_works_brief()
        return {"works": works, "key": key, "token": token}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}
    

@app.post("/create_work")
async def create_work(payload: dict, key: str | None = None, token: str| None = None):
    try:
        works_handler = Works()
        works_handler.add_work(payload['data'])
        return {"msg": "work created", "has_error": False}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}

@app.delete("/delete_work")
async def delete_work(work_id: int, key: str | None = None, token: str| None = None):
    try:
        works_handler = Works()
        works_handler.delete_work(work_id)
        return {"msg": "work deleted", "has_error": False}
    except Exception:
        error = traceback.format_exc()
        logger.error(error)
        return {"error": str(error), "has_error": True}






















@app.get("/get_all_companies")
async def get_all_companies(key: str | None = None, token: str| None = None):
    try:
        return {"companies": [
            {"name": "ABC Corp", "location": "New York"},
            {"name": "XYZ Corp", "location": "California"},
            {"name": "PQR Corp", "location": "Texas"},
            {"name": "MNO Corp", "location": "Florida"},
        ], 
            "key": key,
            "token": token
        }
    except Exception as e:
        return {"error": str(e), "has_error": True}
    

@app.get("/get_company")
async def get_company(company_id: int, key: str | None = None, token: str| None = None):
    try:
        return {"company": {
            "name": "ABC Corp", "location": "New York"
            }, 
            "key": key,
            "token": token
        }
    except Exception as e:
        return {"error": str(e), "has_error": True}


@app.post("/add_company")
async def add_company(company: dict, key: str | None = None, token: str| None = None):
    try:
        return {"company": company, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}


@app.delete("/delete_company")
async def delete_company(company_id: int, key: str | None = None, token: str| None = None):
    try:
        return {"company_id": company_id, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}


@app.post("/create_bill_to_other_company")
async def create_bill_to_other_company(company_id: int, bill_amount: int, key: str | None = None, token: str| None = None):
    try:
        return {"company_id": company_id, "bill_amount": bill_amount, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}

@app.put("/update_bill_of_other_company")
async def update_bill_of_other_company(company_id: int, bill_amount: int, key: str | None = None, token: str| None = None, bill_id: int | None = None):
    try:
        return {"company_id": company_id, "bill_amount": bill_amount, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}
    
@app.delete("/delete_bill_of_other_company")
async def delete_bill_of_other_company(company_id: int, bill_id: int, key: str | None = None, token: str| None = None):
    try:
        return {"company_id": company_id, "bill_id": bill_id, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}

@app.get("/recieve_payment_from_other_company")
async def recieve_payment_from_other_company(company_id: int, payment_amount: int, key: str | None = None, token: str| None = None):
    try:
        return {"company_id": company_id, "payment_amount": payment_amount, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}
    

@app.delete("/delete_payment_from_other_company")
async def delete_payment_from_other_company(company_id: int, payment_id: int, key: str | None = None, token: str| None = None):
    try:
        return {"company_id": company_id, "payment_id": payment_id, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}
    

@app.get("/get_all_loan_details")
async def get_all_loan_details(key: str | None = None, token: str| None = None):
    try:
        return {"loan_details": [
            {"loan_id": 1, "loan_amount": 1000, "interest_rate": 10},
            {"loan_id": 2, "loan_amount": 2000, "interest_rate": 20},
            {"loan_id": 3, "loan_amount": 3000, "interest_rate": 30},
            {"loan_id": 4, "loan_amount": 4000, "interest_rate": 40},
        ], 
            "key": key,
            "token": token
        }
    except Exception as e:
        return {"error": str(e), "has_error": True}


@app.get("/get_loan_detail")
async def get_loan_detail(loan_id: int, key: str | None = None, token: str| None = None):
    try:
        return {"loan_detail": {
            "loan_id": 1, "loan_amount": 1000, "interest_rate": 10
            }, 
            "key": key,
            "token": token
        }
    except Exception as e:
        return {"error": str(e), "has_error": True}
    
@app.post("/create_loan_payment")
async def create_loan_payment(loan_id: int, payment_amount: int, key: str | None = None, token: str| None = None):
    try:
        return {"loan_id": loan_id, "payment_amount": payment_amount, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}

@app.delete("/delete_loan_payment")
async def delete_loan_payment(loan_id: int, payment_id: int, key: str | None = None, token: str| None = None):
    try:
        return {"loan_id": loan_id, "payment_id": payment_id, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}

@app.put("/update_loan_payment")
async def update_loan_payment(loan_id: int, payment_amount: int, key: str | None = None, token: str| None = None, payment_id: int | None = None):
    try:
        return {"loan_id": loan_id, "payment_amount": payment_amount, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}
    

@app.get("/sign_in")
async def sign_in(username: str, password: str, key: str | None = None, token: str| None = None):
    try:
        return {"username": username, "password": password, "key": key, "token": token}
    except Exception as e:
        return {"error": str(e), "has_error": True}
    


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info", access_log=True)