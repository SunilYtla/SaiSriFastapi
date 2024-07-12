import sqlite3
import traceback
import connectorx as cx

class DatabaseInterface:
    def __init__(self, db_name: str):
        self.db_name = db_name

    def execute_with_auto_commit(self, query: str):
        # Connect to the database
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        try:
            # Execute the query
            cursor.execute(query)
            # Commit the transaction
            conn.commit()
            conn.close()
        except Exception:
            conn.close()
            raise

    def execute_select_query(self, query: str):
        # Use ConnectorX to execute the SELECT query
        result = cx.read_sql(f'sqlite://{self.db_name}', query, return_type="polars2")
        return result

# Example usage
if __name__ == "__main__":
    db = DatabaseInterface('example.db')

    # Create table
    db.execute_with_auto_commit('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
    )
    ''')

    # Insert data
    db.execute_with_auto_commit('''
    INSERT INTO users (name, age) VALUES ('Alice', 30)
    ''')

    # Select data
    result = db.execute_select_query('SELECT * FROM users')
    print(result)
