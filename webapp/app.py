from flask import Flask, render_template
import os

app = Flask(__name__)

# Placeholder for listing exercises - will be implemented in the next step
def get_exercises():
    exercise_dir = '../exercises' # Path relative to webapp directory
    if not os.path.exists(exercise_dir) or not os.path.isdir(exercise_dir):
        return []
    # Ensure the path is correctly pointing to the exercises directory from the project root
    # The webapp is in /app/webapp, exercises are in /app/exercises
    actual_exercise_dir = os.path.join(os.path.dirname(__file__), '..', 'exercises')
    if not os.path.exists(actual_exercise_dir) or not os.path.isdir(actual_exercise_dir):
         # Fallback for cases where __file__ might not be standard (e.g. some test environments)
        actual_exercise_dir = 'exercises' # Assuming script is run from project root if ../exercises fails
        if not os.path.exists(actual_exercise_dir) or not os.path.isdir(actual_exercise_dir):
            return []

    return [d for d in os.listdir(actual_exercise_dir) if os.path.isdir(os.path.join(actual_exercise_dir, d))]

@app.route('/')
def index():
    exercises = get_exercises()
    return render_template('index.html', exercises=exercises)

if __name__ == '__main__':
    # Ensure templates and static folders are correctly identified relative to app.py
    # Flask by default looks for 'templates' and 'static' in the application's root path.
    # If app.py is in webapp/, Flask will look for webapp/templates and webapp/static.
    app.run(debug=True, host='0.0.0.0', port=5001)
