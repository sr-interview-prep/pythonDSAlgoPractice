from fastapi.testclient import TestClient
# Adjust the import based on how pytest discovers your app.
# If running pytest from project root: from backend.main import app
# If running pytest from backend/ directory: from main import app (if backend/ is in PYTHONPATH)
# For now, assuming pytest will be run from a context where 'backend.main' is resolvable.
from backend.main import app
import os
import shutil # For potential cleanup, though avoiding in-test file manipulation if possible

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the SQL Practice Platform API"}

def test_list_exercises_success():
    """
    Tests the /api/exercises endpoint.
    Relies on the 'competition_winners' exercise existing in the actual 'exercises' directory,
    as set up in previous steps.
    This makes it more of an integration test for this endpoint's interaction with the filesystem.
    """
    # The path logic in main.py for exercises_dir is:
    # os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "exercises"))
    # where __file__ is backend/main.py. So it correctly points to project_root/exercises.

    response = client.get("/api/exercises")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    # Check if 'competition_winners' is in the list (assuming it was created in prior steps)
    found = any(item['name'] == 'competition_winners' for item in data)
    assert found, "Exercise 'competition_winners' not found. Ensure it exists in 'project_root/exercises/' for this test."

    if found:
        exercise_data = next(item for item in data if item['name'] == 'competition_winners')
        assert "name" in exercise_data
        assert exercise_data['name'] == 'competition_winners'
        assert "problem_description" in exercise_data
        assert "solution_sql" in exercise_data
        # We can check if the content is not empty, but exact content matching can be brittle.
        assert exercise_data['problem_description'] is not None
        assert exercise_data['solution_sql'] is not None
        assert len(exercise_data['problem_description']) > 0 # Assuming it has some content
        assert len(exercise_data['solution_sql']) > 0    # Assuming it has some content

# Example for a basic query test (can be expanded)
# This test requires the database to be accessible.
# The TestClient does not run the app in a way that connects to Docker's network by default,
# so DB connection for tests needs careful handling (e.g., test DB or mocking get_db_connection).
# For now, let's assume direct DB access for testing this endpoint is a more advanced step.
# We can test the endpoint's contract without full DB integration for now if we mock get_db_connection.

# def test_execute_query_success_mocked_db(mocker):
#     # Mock the get_db_connection to avoid actual DB calls
#     mock_conn = mocker.MagicMock()
#     mock_cursor = mocker.MagicMock()
#     mock_conn.cursor.return_value = mock_cursor
#     mock_cursor.description = [("?column?",)] # Simulate a column
#     mock_cursor.fetchall.return_value = [[1]]
#     mock_cursor.rowcount = 1

#     mocker.patch("backend.main.get_db_connection", return_value=mock_conn)

#     response = client.post("/api/exercises/some_exercise/query", json={"sql_query": "SELECT 1"})
#     assert response.status_code == 200
#     data = response.json()
#     assert data["columns"] == ["?column?"]
#     assert data["rows"] == [[1]]
#     assert data["error"] is None
#     mock_conn.commit.assert_called_once() # Ensure commit was called

# def test_execute_empty_query():
#     response = client.post("/api/exercises/test_exercise/query", json={"sql_query": "   "})
#     assert response.status_code == 200 # The endpoint itself doesn't return 400, it returns QueryResult
#     data = response.json()
#     assert data["error"] == "SQL query cannot be empty."
#     assert data["columns"] is None
#     assert data["rows"] is None

# def test_execute_query_db_error_mocked_db(mocker):
#     # Mock get_db_connection to simulate a DB error
#     mock_conn = mocker.MagicMock()
#     mock_conn.cursor.side_effect = psycopg2.Error("Simulated DB Error") # Raise psycopg2.Error

#     mocker.patch("backend.main.get_db_connection", return_value=mock_conn)

#     response = client.post("/api/exercises/error_case/query", json={"sql_query": "SELECT text"})
#     assert response.status_code == 200 # Endpoint returns 200 with error in body
#     data = response.json()
#     assert "Database error: Simulated DB Error" in data["error"]
#     assert data["columns"] is None
#     assert data["rows"] is None
#     mock_conn.rollback.assert_called_once() # Ensure rollback was called
