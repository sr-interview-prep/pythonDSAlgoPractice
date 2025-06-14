import psycopg2
import argparse
import os
import glob

EXERCISES_DIR = "exercises"

def execute_query(query):
    """Connects to the PostgreSQL database and executes the given query."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            user="admin",
            password="admin",
            dbname="sqldb"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()  # Commit changes for INSERT, UPDATE, DELETE

        if cursor.description:
            # Fetch column names
            colnames = [desc[0] for desc in cursor.description]
            print("| " + " | ".join(colnames) + " |")
            print("|" + "----|"*len(colnames)) # Print separator for header

            # Fetch and print all results
            results = cursor.fetchall()
            for row in results:
                print("| " + " | ".join(str(x) for x in row) + " |")
        else:
            # This part is reached for DDL, or INSERT/UPDATE/DELETE without RETURNING clause
            print("Query executed successfully. No tabular results to display.")
            if cursor.rowcount != -1:
                print(f"{cursor.rowcount} rows affected.")


    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL or executing query: {e}")
        if conn: # Rollback on error if a transaction was started
            conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def list_exercises():
    """Lists available exercises in the EXERCISES_DIR."""
    print("Available exercises:")
    if not os.path.exists(EXERCISES_DIR) or not os.path.isdir(EXERCISES_DIR):
        print(f"Exercises directory '{EXERCISES_DIR}' not found.")
        return

    exercises = [d for d in os.listdir(EXERCISES_DIR) if os.path.isdir(os.path.join(EXERCISES_DIR, d))]
    if not exercises:
        print("No exercises found.")
        return
    for exercise_name in exercises:
        print(f"- {exercise_name}")

def run_exercise(exercise_name):
    """Runs a specific exercise."""
    problem_file = os.path.join(EXERCISES_DIR, exercise_name, "problem.md")
    solution_file = os.path.join(EXERCISES_DIR, exercise_name, "solution.sql")

    if not os.path.exists(problem_file):
        print(f"Error: Problem description file not found for exercise '{exercise_name}' at {problem_file}")
        return

    if not os.path.exists(solution_file):
        print(f"Error: Solution file not found for exercise '{exercise_name}' at {solution_file}")
        return

    print(f"--- Problem: {exercise_name} ---")
    try:
        with open(problem_file, 'r') as f:
            print(f.read())
    except Exception as e:
        print(f"Error reading problem file {problem_file}: {e}")
        return

    print("\n--- Running Solution ---")
    try:
        with open(solution_file, 'r') as f:
            sql_query = f.read()
        print(f"Executing SQL from {solution_file}: \n{sql_query.strip()}")
        execute_query(sql_query)
    except FileNotFoundError:
        # This case should be caught by the check above, but good to have
        print(f"Error: Solution file not found at path: {solution_file}")
    except Exception as e:
        print(f"Error reading or executing solution file {solution_file}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Execute SQL queries or run exercises against a PostgreSQL database.")

    # Mutually exclusive group for different modes of operation
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list-exercises", action="store_true", help="List available exercises.")
    group.add_argument("--exercise", type=str, help="Name of the exercise to run.")
    group.add_argument("query_string", nargs="?", type=str, help="SQL query string to execute directly.")
    group.add_argument("--file", type=str, help="Path to a .sql file containing the query to execute.")

    args = parser.parse_args()

    if args.list_exercises:
        list_exercises()
    elif args.exercise:
        run_exercise(args.exercise)
    elif args.query_string:
        print(f"Executing SQL Query: \n{args.query_string.strip()}")
        execute_query(args.query_string)
    elif args.file:
        try:
            with open(args.file, 'r') as f:
                sql_query = f.read()
            print(f"Executing SQL from file {args.file}: \n{sql_query.strip()}")
            execute_query(sql_query)
        except FileNotFoundError:
            print(f"Error: File not found at path: {args.file}")
        except Exception as e:
            print(f"Error reading file {args.file}: {e}")
    else:
        # This case should not be reached due to the mutually exclusive group being required
        parser.print_help()


if __name__ == "__main__":
    main()
