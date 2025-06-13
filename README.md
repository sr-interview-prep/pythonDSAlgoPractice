# SQL Practice Platform

This project provides a full-stack platform for practicing SQL queries, similar to a LeetCode-style environment. It features a PostgreSQL database, a FastAPI backend, and a React (TypeScript) frontend, all orchestrated with Docker Compose.

## Features

-   Dockerized PostgreSQL database with schema/data initialization.
-   FastAPI backend providing RESTful APIs for:
    -   Listing SQL exercises (problem description, solution template).
    -   Executing user-submitted SQL queries against the database.
-   React (TypeScript) frontend with Vite for:
    -   Displaying exercise lists and problem descriptions (Markdown rendered).
    -   A SQL editor area for users to write and submit queries.
    -   Displaying query results (tables, messages, or errors).
-   Multi-container setup using Docker Compose for easy startup of all services.
-   Organized structure for adding new SQL exercises.
-   Unit tests for both backend and frontend.
-   CORS configured for local development.

## Project Structure

```
.
├── backend/                # FastAPI backend application
│   ├── Dockerfile          # Dockerfile for the backend
│   ├── main.py             # Main FastAPI application logic
│   ├── tests/              # Backend unit tests
│   │   └── test_main.py
│   ├── __init__.py
│   └── ...
├── exercises/              # SQL exercises, each in its own directory
│   └── competition_winners/ # Example exercise
│       ├── problem.md      # Problem description in Markdown
│       └── solution.sql    # Model solution SQL
├── frontend/               # React TypeScript frontend application (Vite)
│   ├── Dockerfile          # Dockerfile for the frontend (multi-stage build with Nginx)
│   ├── nginx.conf          # Nginx configuration for serving frontend
│   ├── public/             # Static assets for the frontend
│   ├── src/                # Frontend source code
│   │   ├── components/     # React components
│   │   │   ├── ExerciseDetail.tsx
│   │   │   └── ExerciseList.tsx
│   │   ├── App.tsx         # Main App component
│   │   ├── App.test.tsx    # Unit tests for App component
│   │   └── setupTests.ts   # Test setup for Vitest (e.g., jest-dom matchers)
│   ├── package.json        # Frontend dependencies and scripts
│   ├── vite.config.ts      # Vite configuration (includes Vitest config)
│   └── tsconfig.json       # TypeScript configuration for frontend
├── sql_init/               # Database initialization scripts
│   └── init.sql            # SQL script to create tables and insert initial data
├── .gitignore              # Specifies intentionally untracked files
├── docker-compose.yml      # Docker Compose configuration for all services
├── Dockerfile              # Original Dockerfile for PostgreSQL (now unused, official image preferred)
├── README.md               # This file
└── requirements.txt        # Python dependencies for the FastAPI backend
```

## Prerequisites

-   **Docker & Docker Compose:** For running the entire application stack.
    -   Install Docker: [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/)
    -   Install Docker Compose: [https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/)
-   **Python 3.8+ & pip:** For local backend development and running backend tests.
    -   Install Python: [https://www.python.org/downloads/](https://www.python.org/downloads/)
-   **Node.js (LTS version recommended, e.g., 18.x or 20.x) & npm:** For local frontend development and running frontend tests.
    -   Install Node.js (includes npm): [https://nodejs.org/](https://nodejs.org/)

## Setup and Running the Platform (Docker Compose - Recommended)

This is the primary method to run the entire application (Database, Backend, Frontend).

1.  **Clone the repository and navigate to the project root.**
2.  **Build and Start All Services:**
    Open your terminal in the project root directory and run:
    ```bash
    docker-compose up --build
    ```
    -   `--build` forces Docker Compose to build the images for the backend and frontend services before starting them.
    -   This command will start the PostgreSQL database, the FastAPI backend, and the Nginx server for the React frontend.
    -   You can add `-d` to run in detached mode (`docker-compose up --build -d`).

3.  **Accessing the Services:**
    -   **Frontend Application:** Open your browser to `http://localhost:5173`
    -   **Backend API (FastAPI):** Available at `http://localhost:8000`
        -   API Docs (Swagger UI): `http://localhost:8000/docs`
        -   Alternative API Docs (ReDoc): `http://localhost:8000/redoc`
    -   **PostgreSQL Database:** Accessible on `localhost:5432` from your host machine (e.g., for a DB client).

## Local Development Setup

This setup is for when you want to actively develop the frontend or backend and see changes live without rebuilding Docker images for each change.

**A. Start the Database (Required for both Backend/Frontend local dev):**
   Ensure the PostgreSQL database is running via Docker Compose:
   ```bash
   docker-compose up -d postgres_db
   ```
   -   This ensures the database is available on `localhost:5432`.

**B. Frontend Development (Vite Dev Server):**
   1.  Navigate to the frontend directory: `cd frontend`
   2.  Install dependencies (if not already done): `npm install`
   3.  Start the Vite development server: `npm run dev`
   -   The frontend will typically be available at `http://localhost:5173`.
   -   **API Connection:** The frontend (in `src/App.tsx`) is configured to connect to the backend API at `http://localhost:8000`. This will work if your backend (either local or Dockerized) is accessible on that port.
       -   You can configure the API base URL in the frontend code if needed, or use a `.env` file with a variable like `VITE_API_BASE_URL=http://your_backend_url` and update `axios` calls to use `import.meta.env.VITE_API_BASE_URL`.

**C. Backend Development (Uvicorn Dev Server):**
   1.  Navigate to the backend directory: `cd backend`
   2.  Install/update Python dependencies (if not already done): `pip install -r ../requirements.txt` (note the path to `requirements.txt` is from the `backend` directory).
   3.  Run the FastAPI development server: `uvicorn main:app --reload --port 8000`
   -   The backend API will be available at `http://localhost:8000`.
   -   It will connect to the PostgreSQL database running in Docker on `localhost:5432`. The `backend/main.py` script checks the `RUNNING_IN_DOCKER` environment variable (which is not set in this local setup) and correctly defaults to `localhost` for the database host.

## Data Schema Updates & Adding New SQL Exercises

1.  **Create Exercise Files:**
    -   Create a new directory under `exercises/<new_exercise_name>/`.
    -   Inside this new directory, add:
        -   `problem.md`: The exercise description in Markdown.
        -   `solution.sql`: The model SQL solution.
2.  **Update Database Initialization (If new schema/data is needed):**
    -   Modify `sql_init/init.sql` to include any new `CREATE TABLE` or `INSERT INTO` statements.
    -   These scripts are executed when the `postgres_db` service starts for the first time or after its data volume is removed.
3.  **Apply Changes:**
    -   If you only added new exercise files in `exercises/` without changing `sql_init/init.sql`, and your backend/frontend are running via Docker Compose, you might need to restart the backend container to pick up new exercises, or if running locally, the Uvicorn server often reloads. The frontend will fetch the new list.
    -   If you changed `sql_init/init.sql`, you must reset the database for the changes to apply:
        ```bash
        docker-compose down -v postgres_db # Stops and removes the postgres container and its volume
        docker-compose up -d postgres_db    # Restarts and reinitializes the database
        # Or for all services: docker-compose down -v && docker-compose up --build -d
        ```

## Running Unit Tests

**Backend Tests (pytest):**
1.  Navigate to the backend directory: `cd backend`
2.  Ensure Python dependencies, including `pytest`, are installed: `pip install -r ../requirements.txt`
3.  Run tests: `pytest`

**Frontend Tests (Vitest):**
1.  Navigate to the frontend directory: `cd frontend`
2.  Ensure Node.js dependencies are installed: `npm install`
3.  Run tests: `npm test` (or `npm run test`)

## Managing Docker Compose Services

These commands should be run from the project root directory.

-   **List running services:** `docker-compose ps`
-   **Stop all services:** `docker-compose stop` (to stop specific services: `docker-compose stop backend frontend`)
-   **Stop and remove containers:** `docker-compose down` (add `-v` to also remove volumes, e.g., `docker-compose down -v` for a full reset)
-   **View logs for all services:** `docker-compose logs -f`
-   **View logs for a specific service:** `docker-compose logs -f backend`

---
Happy SQL Practicing!
```
