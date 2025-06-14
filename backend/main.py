from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware # Added import
from pydantic import BaseModel
import os
import psycopg2
import psycopg2.extras
from typing import List, Optional, Any

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
                except Exception as e:
                    description = f"Error reading problem description: {e}"

            solution = "Solution SQL not found."
            if os.path.exists(solution_sql_path):
                try:
                    with open(solution_sql_path, "r", encoding="utf-8") as f:
                        solution = f.read()
                except Exception as e:
                    solution = f"Error reading solution SQL: {e}"

            exercise_list.append(
                Exercise(
                    name=exercise_name,
                    problem_description=description,
                    solution_sql=solution,
                )
            )
    return exercise_list

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
