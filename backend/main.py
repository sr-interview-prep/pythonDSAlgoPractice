from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware # Added import
from pydantic import BaseModel
import os
import psycopg2
import psycopg2.extras
from typing import List, Optional, Any
import re # Added import
import csv # Added import

# --- FastAPI App Initialization ---
app = FastAPI(title="SQL Practice Platform API")

# --- CORS Configuration ---
# Define allowed origins for development.
# For production, these should ideally be configurable via environment variables.
origins = [
    "http://localhost",         # Common base for local development
    "http://localhost:3000",    # Common React dev port
    "http://localhost:5173",    # Default Vite dev port for React/Vue
    "http://127.0.0.1",         # Explicit loopback IP
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       # List of origins that are allowed to make cross-origin requests.
    allow_credentials=True,      # Indicates that cookies should be supported for cross-origin requests.
    allow_methods=["*"],         # A list of HTTP methods that are allowed. Using ["*"] allows all standard methods.
    allow_headers=["*"],         # A list of HTTP request headers that should be supported. Using ["*"] allows all.
)

# --- Pydantic Models ---
class Exercise(BaseModel):
    name: str
    problem_description: Optional[str] = None
    solution_sql: Optional[str] = None
    ddl_statements: Optional[List[str]] = None
    csv_files: Optional[List[str]] = None
    output_csv_content: Optional[List[List[Any]]] = None

class SQLQueryRequest(BaseModel):
    sql_query: str

class QueryResult(BaseModel):
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    error: Optional[str] = None
    message: Optional[str] = None

# --- Database Configuration ---
# DB_HOST will be determined dynamically based on environment
DB_PORT = "5432"
DB_USER = "admin"
DB_PASS = "admin"
DB_NAME = "sqldb"

# --- Helper Functions ---
def get_db_connection():
    db_host = "postgres_db" if os.getenv("RUNNING_IN_DOCKER") == "true" else "localhost"
    try:
        conn = psycopg2.connect(
            host=db_host, # Use dynamically determined host
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        return conn
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database connection unavailable: {e}")


# --- API Endpoints ---
@app.get("/")
async def read_root():
    return {"message": "Welcome to the SQL Practice Platform API"}

@app.get("/api/exercises", response_model=List[Exercise])
async def list_exercises():
    exercises_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "exercises"))

    if not os.path.exists(exercises_dir) or not os.path.isdir(exercises_dir):
        return []

    exercise_list = []
    for exercise_name in sorted(os.listdir(exercises_dir)):
        exercise_path = os.path.join(exercises_dir, exercise_name)
        if os.path.isdir(exercise_path):
            problem_md_path = os.path.join(exercise_path, "problem.md")
            solution_sql_path = os.path.join(exercise_path, "solution.sql")

            description = "Problem description not found."
            if os.path.exists(problem_md_path):
                try:
                    with open(problem_md_path, "r", encoding="utf-8") as f:
                        description = f.read()

                    # Parse DDL statements and CSV files from description
                    ddl_statements = []
                    # Regex to find CREATE TABLE statements (multi-line)
                    # It looks for "CREATE TABLE" and captures until a semicolon ";"
                    # re.DOTALL makes . match newlines as well
                    # re.IGNORECASE makes the search case-insensitive
                    for match in re.finditer(r"CREATE TABLE.*?;", description, re.DOTALL | re.IGNORECASE):
                        ddl_statements.append(match.group(0).strip())

                    csv_files = []
                    # Regex to find CSV file mentions like "`students.csv`"
                    # It looks for backticks around a filename ending with .csv
                    for match in re.finditer(r"`([\w-]+\.csv)`", description):
                        csv_files.append(match.group(1))

                except Exception as e:
                    description = f"Error reading problem description: {e}"
                    ddl_statements = []
                    csv_files = []

            solution = "Solution SQL not found."
            if os.path.exists(solution_sql_path):
                try:
                    with open(solution_sql_path, "r", encoding="utf-8") as f:
                        solution = f.read()
                except Exception as e:
                    solution = f"Error reading solution SQL: {e}"

            output_csv_data = None
            output_csv_path = os.path.join(exercise_path, "output.csv")
            if os.path.exists(output_csv_path):
                try:
                    with open(output_csv_path, "r", encoding="utf-8") as f_csv:
                        reader = csv.reader(f_csv)
                        output_csv_data = [row for row in reader] # Reads all rows, including header
                except Exception as e:
                    # Log error or handle as appropriate for your application
                    print(f"Error reading output.csv for exercise {exercise_name}: {e}")
                    output_csv_data = None # Ensure it's None if reading failed

            exercise_list.append(
                Exercise(
                    name=exercise_name,
                    problem_description=description,
                    solution_sql=solution,
                    ddl_statements=ddl_statements if ddl_statements else None,
                    csv_files=csv_files if csv_files else None,
                    output_csv_content=output_csv_data,
                )
            )
    return exercise_list


async def get_exercise_details(exercise_name: str) -> Exercise:
    """Helper function to get exercise details including DDL and CSV files."""
    exercises_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "exercises"))
    exercise_path = os.path.join(exercises_dir, exercise_name)

    if not os.path.isdir(exercise_path):
        raise HTTPException(status_code=404, detail=f"Exercise '{exercise_name}' not found.")

    problem_md_path = os.path.join(exercise_path, "problem.md")
    solution_sql_path = os.path.join(exercise_path, "solution.sql")
    output_csv_path = os.path.join(exercise_path, "output.csv") # Path for output.csv

    description = "Problem description not found."
    ddl_statements = []
    csv_files = []
    output_csv_data = None

    if os.path.exists(problem_md_path):
        try:
            with open(problem_md_path, "r", encoding="utf-8") as f:
                description = f.read()

            for match in re.finditer(r"CREATE TABLE.*?;", description, re.DOTALL | re.IGNORECASE):
                ddl_statements.append(match.group(0).strip())

            for match in re.finditer(r"`([\w-]+\.csv)`", description):
                csv_files.append(match.group(1))
        except Exception as e:
            description = f"Error reading problem description: {e}" # Keep error in description for now
            # ddl_statements and csv_files will remain empty or as previously parsed before error

    solution = "Solution SQL not found."
    if os.path.exists(solution_sql_path):
        try:
            with open(solution_sql_path, "r", encoding="utf-8") as f:
                solution = f.read()
        except Exception as e:
            solution = f"Error reading solution SQL: {e}"

    if os.path.exists(output_csv_path):
        try:
            with open(output_csv_path, "r", encoding="utf-8") as f_csv:
                reader = csv.reader(f_csv)
                output_csv_data = [row for row in reader]
        except Exception as e:
            # Log or handle error appropriately
            print(f"Error reading output.csv for exercise {exercise_name} in get_exercise_details: {e}")
            # output_csv_data remains None

    return Exercise(
        name=exercise_name,
        problem_description=description,
        solution_sql=solution,
        ddl_statements=ddl_statements if ddl_statements else None,
        csv_files=csv_files if csv_files else None,
        output_csv_content=output_csv_data,
    )

@app.post("/api/exercises/{exercise_name}/prepare", response_model=QueryResult)
async def prepare_exercise_data(exercise_name: str):
    conn = None
    cursor = None
    try:
        exercise_details = await get_exercise_details(exercise_name)

        if not exercise_details.ddl_statements:
            return QueryResult(message="No DDL statements found for this exercise. No preparation needed or problem.md is missing DDLs.")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Drop tables defined in DDLs first
        for ddl in exercise_details.ddl_statements:
            match = re.search(r"CREATE TABLE\s+IF NOT EXISTS\s+(\w+)", ddl, re.IGNORECASE)
            if not match: # Try without IF NOT EXISTS
                 match = re.search(r"CREATE TABLE\s+(\w+)", ddl, re.IGNORECASE)

            if match:
                table_name = match.group(1)
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;") # Use CASCADE to drop dependent objects
                    conn.commit()
                except psycopg2.Error as e:
                    conn.rollback()
                    # If table doesn't exist, it's fine, otherwise raise error
                    if e.pgcode != '42P01': # 42P01 is undefined_table
                        raise HTTPException(status_code=500, detail=f"Error dropping table {table_name}: {e.pgerror}")


        # Execute DDL statements to create tables
        for ddl in exercise_details.ddl_statements:
            try:
                cursor.execute(ddl)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                raise HTTPException(status_code=500, detail=f"Error executing DDL: {e.pgerror}\nDDL: {ddl}")

        # Load data from CSV files
        if exercise_details.csv_files:
            exercise_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "exercises", exercise_name))
            for csv_file_name in exercise_details.csv_files:
                csv_path = os.path.join(exercise_base_path, csv_file_name)
                table_name_from_csv = os.path.splitext(csv_file_name)[0].upper() # e.g., STUDENTS from students.csv

                # Attempt to find the actual table name from DDLs in case of case differences or aliasing
                actual_table_name = None
                for ddl in exercise_details.ddl_statements:
                    match = re.search(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)", ddl, re.IGNORECASE)
                    if match:
                        parsed_table_name = match.group(1)
                        if parsed_table_name.upper() == table_name_from_csv:
                            actual_table_name = parsed_table_name
                            break

                if not actual_table_name:
                    # Fallback if no matching DDL table name is found (should ideally not happen if problem.md is correct)
                    actual_table_name = table_name_from_csv


                if not os.path.exists(csv_path):
                    raise HTTPException(status_code=404, detail=f"CSV file {csv_file_name} not found at {csv_path}")

                try:
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        headers = next(reader)

                        # Ensure headers are quoted if they are SQL keywords or contain special characters
                        # For simplicity, we'll quote all headers. PostgreSQL will handle it.
                        quoted_headers = [f'"{h.strip()}"' for h in headers]

                        for row in reader:
                            if not row: continue # Skip empty rows
                            # Basic quoting for string values. Numbers should not be quoted.
                            # More robust type detection might be needed for production.
                            values = []
                            for val in row:
                                if val is None: # Handle NULL values
                                    values.append("NULL")
                                else:
                                    # Attempt to convert to number, if fails, treat as string
                                    try:
                                        float(val) # Check if it's a number
                                        values.append(val) # Keep as is for psycopg2 to handle
                                    except ValueError:
                                        # Escape single quotes in string data
                                        escaped_val = val.replace("'", "''")
                                        values.append(f"'{escaped_val}'")

                            insert_query = f"INSERT INTO {actual_table_name} ({', '.join(quoted_headers)}) VALUES ({', '.join(values)});"
                            try:
                                cursor.execute(insert_query)
                            except psycopg2.Error as e:
                                conn.rollback()
                                raise HTTPException(status_code=500, detail=f"Error inserting data from {csv_file_name} into {actual_table_name}: {e.pgerror}\nQuery: {insert_query}")
                    conn.commit()
                except FileNotFoundError:
                    raise HTTPException(status_code=404, detail=f"CSV file {csv_file_name} not found.")
                except Exception as e:
                    conn.rollback()
                    raise HTTPException(status_code=500, detail=f"Error processing CSV file {csv_file_name}: {str(e)}")

        return QueryResult(message=f"Exercise '{exercise_name}' data prepared successfully. Tables created and data loaded.")

    except HTTPException as http_exc:
        # Re-raise HTTPExceptions to be handled by FastAPI
        if conn: conn.rollback()
        raise http_exc
    except psycopg2.Error as db_err:
        if conn: conn.rollback()
        return QueryResult(error=f"Database error during preparation: {db_err.pgerror if hasattr(db_err, 'pgerror') else str(db_err)}")
    except Exception as e:
        if conn: conn.rollback()
        return QueryResult(error=f"An unexpected error occurred during preparation: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.post("/api/exercises/{exercise_name}/query", response_model=QueryResult)
async def execute_sql_query(exercise_name: str, query_request: SQLQueryRequest):
    conn = None
    cursor = None

    if not query_request.sql_query or query_request.sql_query.strip() == "":
        return QueryResult(error="SQL query cannot be empty.")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(query_request.sql_query)

        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            conn.commit()
            return QueryResult(columns=columns, rows=rows)
        else:
            conn.commit()
            rowcount = cursor.rowcount if cursor.rowcount != -1 else 0
            return QueryResult(message=f"Query executed successfully. {rowcount} rows affected.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        if hasattr(e, 'pgcode') and e.pgcode == '42601':
             raise HTTPException(status_code=400, detail=f"SQL Syntax Error: {e.pgerror if hasattr(e, 'pgerror') else str(e)}")
        return QueryResult(error=f"Database error: {e.pgerror if hasattr(e, 'pgerror') else str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        return QueryResult(error=f"An unexpected error occurred: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
