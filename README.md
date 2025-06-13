# SQL Practice Platform

This project provides a full-stack platform for practicing SQL queries, similar to a LeetCode-style environment. It features a PostgreSQL database, a FastAPI backend, and a React (TypeScript) frontend.

## Features

-   Dockerized PostgreSQL database with schema/data initialization.
-   FastAPI backend providing RESTful APIs for:
    -   Listing SQL exercises (problem description, solution template).
    -   Executing user-submitted SQL queries against the database.
-   React (TypeScript) frontend with Vite for:
    -   Displaying exercise lists and problem descriptions (Markdown rendered).
    -   A SQL editor area for users to write and submit queries.
    -   Displaying query results (tables, messages, or errors).
-   Organized structure for adding new SQL exercises.
-   CORS configured for local development, allowing frontend and backend to communicate.

## Project Structure

```
.
├── backend/            # FastAPI backend application
│   ├── main.py         # Main FastAPI application logic
│   └── ...             # Other backend files (e.g., models, routers if refactored)
├── exercises/          # SQL exercises, each in its own directory
│   └── competition_winners/ # Example exercise
│       ├── problem.md  # Problem description in Markdown
│       └── solution.sql# Model solution SQL
├── frontend/           # React TypeScript frontend application (Vite)
│   ├── public/         # Static assets for the frontend
│   ├── src/            # Frontend source code (components, App.tsx, etc.)
│   ├── package.json    # Frontend dependencies and scripts
│   ├── vite.config.ts  # Vite configuration
│   └── tsconfig.json   # TypeScript configuration for frontend
├── sql_init/           # Database initialization scripts
│   └── init.sql        # SQL script to create tables and insert initial data
├── .gitignore          # Specifies intentionally untracked files
├── docker-compose.yml  # Docker configuration for the PostgreSQL service
├── Dockerfile          # Dockerfile for PostgreSQL (official image is used, so this is minimal)
├── README.md           # This file
└── requirements.txt    # Python dependencies for the FastAPI backend
```

## Prerequisites

-   **Docker & Docker Compose:** For running the PostgreSQL database.
    -   Install Docker: [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/)
    -   Install Docker Compose: [https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/)
-   **Python 3.8+ & pip:** For the FastAPI backend.
    -   Install Python: [https://www.python.org/downloads/](https://www.python.org/downloads/)
-   **Node.js (LTS version recommended, e.g., 18.x or 20.x) & npm:** For the React frontend.
    -   Install Node.js (includes npm): [https://nodejs.org/](https://nodejs.org/)

## Setup and Running the Platform

There are three main components to run: PostgreSQL Database, Backend API, and Frontend Application. Ensure Docker daemon is running before you start.

**1. Start the PostgreSQL Database:**

   Open your terminal and navigate to the project root directory. Run:
   ```bash
   docker-compose up -d postgres_db
   ```
   - This command starts the PostgreSQL container in detached mode (`-d`).
   - On the first run, Docker will pull the `postgres:latest` image.
   - The database is initialized with schemas and data from `sql_init/init.sql` on its first run or if the volume is cleared.
   - **Database credentials** (used by the FastAPI backend):
     - Host: `localhost` (from the perspective of your machine; `postgres_db` if services were in the same docker network)
     - Port: `5432`
     - User: `admin`
     - Password: `admin`
     - Database Name: `sqldb`
   - Data is persisted in the `./postgres-data` directory (created by Docker Compose on your host machine).

**2. Start the FastAPI Backend:**

   In a new terminal window/tab, navigate to the project root directory:
   ```bash
   # Install/update Python dependencies (if you haven't already or if requirements.txt changed)
   pip install -r requirements.txt

   # Navigate to the backend directory
   cd backend

   # Run the FastAPI development server using Uvicorn
   # --reload enables auto-reload on code changes
   # --port 8000 specifies the port (backend API configured to be accessible here)
   uvicorn main:app --reload --port 8000
   ```
   - The backend API will be available at `http://localhost:8000`.
   - You can explore the interactive API documentation (Swagger UI) at `http://localhost:8000/docs`.
   - And alternative ReDoc documentation at `http://localhost:8000/redoc`.

**3. Start the React Frontend:**

   In another new terminal window/tab, navigate to the `frontend` directory:
   ```bash
   cd frontend

   # Install frontend dependencies (if you haven't already or if package.json changed)
   npm install

   # Start the Vite development server
   npm run dev
   ```
   - The Vite development server will start, and your terminal output will show the URL where the frontend application is being served (typically `http://localhost:5173`).
   - Open this URL in your web browser to use the SQL practice platform.

## Adding New SQL Exercises

1.  **Create Exercise Directory:**
    Create a new directory under `exercises/`. The name of this directory will be used as the exercise identifier (e.g., `exercises/my_new_sql_challenge/`).

2.  **Add Problem Description:**
    Inside your new exercise directory, create a `problem.md` file. Describe the problem, the relevant table schemas, and the expected output or task using Markdown.

3.  **Add Solution SQL:**
    Also inside the exercise directory, create a `solution.sql` file. This file should contain the model SQL solution for the exercise. This solution will be fetched by the backend and can be used by the frontend (e.g., to pre-fill the SQL editor).

4.  **Update Database Initialization (If Needed):**
    If your new exercise requires new tables, views, or initial seed data that are not already part of `sql_init/init.sql`:
    -   Add the necessary `CREATE TABLE ...;`, `INSERT INTO ...;`, etc., statements to the `sql_init/init.sql` file.
    -   These statements will be executed by the PostgreSQL container when it's first created or after a full reset (see "Resetting the database" below). This ensures that the database schema and data are ready for your new exercise.
    -   **Note:** The scripts in `/docker-entrypoint-initdb.d/` (where `sql_init/init.sql` is mounted) are executed in alphabetical order.

## Managing the Database Container

These commands should be run from the project root directory where `docker-compose.yml` is located.

-   **Stopping the container:**
    To stop the `postgres_db` service without removing its data:
    ```bash
    docker-compose stop postgres_db
    ```
    To stop all services defined in `docker-compose.yml` (if more were added):
    ```bash
    docker-compose stop
    ```

-   **Viewing logs:**
    To see the logs from the PostgreSQL container (useful for debugging initialization or runtime errors):
    ```bash
    docker-compose logs postgres_db
    ```
    To follow the logs in real-time:
    ```bash
    docker-compose logs -f postgres_db
    ```

-   **Resetting the database (deletes all data!):**
    If you want to completely reset the database, which means all data in the `./postgres-data` volume will be deleted, and the `init.sql` script will run afresh on the next startup:
    1.  Stop and remove the container and its associated volume:
        ```bash
        docker-compose down -v
        ```
    2.  Then, restart the container. It will be re-initialized using `sql_init/init.sql`:
        ```bash
        docker-compose up -d postgres_db
        ```

---
Happy SQL Practicing!
```
