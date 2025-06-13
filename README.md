# SQL Practice Platform

This platform provides a local environment for practicing SQL queries using PostgreSQL. It includes a set of predefined exercises and allows for running ad-hoc SQL queries.

## Prerequisites

Before you begin, ensure you have the following installed:

*   **Docker and Docker Compose:** To run the PostgreSQL database container.
    *   [Install Docker](https://docs.docker.com/get-docker/)
    *   [Install Docker Compose](https://docs.docker.com/compose/install/)
*   **Python 3.x and pip:** To run the helper scripts and install dependencies.
    *   [Install Python](https://www.python.org/downloads/)

## Setup

Follow these steps to set up the environment:

1.  **Start the PostgreSQL Database:**
    Open your terminal and navigate to the root directory of this project. Then run:
    ```bash
    docker-compose up -d postgres_db
    ```
    This command starts the PostgreSQL container in detached mode (`-d`). On the first run, Docker will pull the `postgres:latest` image. The database will be automatically initialized with any schemas and data defined in `sql_init/init.sql`.

2.  **Install Python Dependencies:**
    Install the necessary Python packages using pip and the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```

## Running SQL Queries

The `run_sql.py` script is used to interact with the database and run exercises.

*   **Base command:** `python run_sql.py`

*   **List Available Exercises:**
    To see a list of all available SQL exercises:
    ```bash
    python run_sql.py --list-exercises
    ```

*   **Run a Specific Exercise:**
    To run a particular exercise, use its name (which corresponds to the directory name under `exercises/`):
    ```bash
    python run_sql.py --exercise <exercise_name>
    ```
    For example:
    ```bash
    python run_sql.py --exercise competition_winners
    ```
    This will first display the problem description from `problem.md` and then execute the corresponding `solution.sql`, printing the results.

*   **Run an Ad-hoc SQL Query String:**
    You can execute a SQL query directly from the command line:
    ```bash
    python run_sql.py "SELECT * FROM STUDENTS;"
    ```
    Make sure to enclose your SQL query in quotes.

*   **Run an SQL Query from a File:**
    To execute a query stored in a `.sql` file:
    ```bash
    python run_sql.py --file path/to/your_query.sql
    ```

## Database Details

The PostgreSQL database is configured as follows:

*   **Host:** `localhost`
*   **Port:** `5432`
*   **User:** `admin`
*   **Password:** `admin`
*   **Database Name:** `sqldb`

These credentials are defined in the `docker-compose.yml` file. Database data is persisted in the `./postgres-data` directory on your host machine. This means your data will remain even if you stop and start the container, but not if you remove the volume (see below).

## Managing the Database Container

You can manage the PostgreSQL container using `docker-compose` commands:

*   **Stopping the container:**
    To stop the running `postgres_db` service without removing its data:
    ```bash
    docker-compose stop postgres_db
    ```

*   **Viewing logs:**
    To see the logs from the PostgreSQL container (useful for debugging initialization or runtime errors):
    ```bash
    docker-compose logs postgres_db
    ```
    You can follow the logs in real-time using `docker-compose logs -f postgres_db`.

*   **Resetting the database (deletes all data!):**
    If you want to completely reset the database, including all data, tables, and schemas defined in `init.sql`:
    1.  Stop and remove the container and its associated volume:
        ```bash
        docker-compose down -v
        ```
    2.  Then, restart the container. It will be re-initialized using `sql_init/init.sql`:
        ```bash
        docker-compose up -d postgres_db
        ```

## Adding New SQL Exercises

To add a new SQL exercise to the platform:

1.  **Create a Directory:**
    Create a new directory for your exercise under the `exercises/` folder. The name of this directory will be used as `<exercise_name>`.
    Example: `exercises/new_data_analysis_challenge/`

2.  **Add Problem Description:**
    Inside your new exercise directory, create a `problem.md` file. Describe the problem, the schema involved, and the expected output or task.

3.  **Add Solution File:**
    Also inside your new exercise directory, create a `solution.sql` file containing the model SQL solution for the problem.

4.  **Update Initialization Script (if new schema/data is needed):**
    If your new exercise requires new tables, views, or initial seed data:
    *   Add the necessary `CREATE TABLE ...;`, `INSERT INTO ...;`, etc., statements to the `sql_init/init.sql` file.
    *   These statements will be executed when the database is first created or after a reset (using `docker-compose down -v` and then `up`). This ensures the necessary database objects and data are available for your exercise.
    *   **Important:** Try to make your DDL and DML statements idempotent if possible (e.g., using `CREATE TABLE IF NOT EXISTS`), though the `/docker-entrypoint-initdb.d/` mechanism in the postgres image typically runs scripts only once if the data directory is persisted and already initialized. A full reset (`docker-compose down -v`) guarantees a fresh run of all init scripts.
