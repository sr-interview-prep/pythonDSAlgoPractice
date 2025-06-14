import React, { useState } from 'react'; // Removed useEffect as it's not used here directly
import axios from 'axios';
import ExerciseList from './components/ExerciseList';
import ExerciseDetail from './components/ExerciseDetail'; // Corrected path if ExerciseDetail is in components
import './App.css'; // Basic global styles

// Define the Exercise structure (shared across components)
export interface Exercise { // Exporting for potential use in other files if needed
  name: string;
  problem_description: string | null;
  solution_sql: string | null;
}

// Define the QueryResult structure (shared)
export interface QueryResult { // Exporting for potential use
  columns?: string[];
  rows?: any[][];
  error?: string;
  message?: string;
}

const App: React.FC = () => {
  const [selectedExercise, setSelectedExercise] = useState<Exercise | null>(null);
  // Current query result is managed within ExerciseDetail, App only provides the function to run query

  const handleSelectExercise = (exercise: Exercise) => {
    setSelectedExercise(exercise);
  };

  const handleBackToList = () => {
    setSelectedExercise(null);
  };

  const handleRunQuery = async (exerciseName: string, sqlQuery: string): Promise<QueryResult> => {
    try {
      // Backend API is running on http://localhost:8000
      const response = await axios.post<QueryResult>(
        `http://localhost:8000/api/exercises/${exerciseName}/query`,
        { sql_query: sqlQuery } // Request body
      );
      return response.data;
    } catch (err) {
      // Handle various types of Axios errors
      if (axios.isAxiosError(err)) {
        if (err.response) {
          // The request was made and the server responded with a status code
          // that falls out of the range of 2xx.
          // The backend might send a QueryResult-like structure in err.response.data for specific errors.
          if (err.response.data && (err.response.data.error || err.response.data.message)) {
            return err.response.data as QueryResult;
          }
          return { error: `Server error: ${err.response.status} - ${err.response.data.detail || err.response.statusText}` };
        } else if (err.request) {
          // The request was made but no response was received
          return { error: 'No response from server. Please check if the backend is running.' };
        } else {
          // Something happened in setting up the request that triggered an Error
          return { error: `Request setup error: ${err.message}` };
        }
      }
      // For non-Axios errors or other unexpected issues
      return { error: 'An unexpected error occurred during query execution.' };
    }
  };

  return (
    <div className="App">
      <header className="App-header" style={{ backgroundColor: '#282c34', padding: '20px', color: 'white', textAlign: 'center' }}>
        <h1>SQL Practice Platform</h1>
      </header>
      {/* The <main> element will grow to fill available space, pushing the footer down */}
      <main style={{ padding: '20px', maxWidth: '1000px', margin: '0 auto', flexGrow: 1, width: '100%' }}>
        {selectedExercise ? (
          <ExerciseDetail
            exercise={selectedExercise}
            onBackToList={handleBackToList}
            onRunQuery={handleRunQuery}
            // queryResult state is now managed within ExerciseDetail, so no need to pass it as prop from App
          />
        ) : (
          // ExerciseList fetches its own data as per its implementation
          <ExerciseList onSelectExercise={handleSelectExercise} />
        )}
      </main>
      {/* The marginTop: 'auto' on the footer will push it to the bottom of the flex container if main doesn't fill all space */}
      <footer style={{ textAlign: 'center', padding: '20px', marginTop: 'auto', borderTop: '1px solid #eee', color: '#777', width: '100%' }}>
        <p>&copy; {new Date().getFullYear()} SQL Practice Platform</p>
      </footer>
    </div>
  );
};

export default App;
