import React, { useState } from 'react';
import axios from 'axios';
import ExerciseList from './components/ExerciseList';
import ExerciseDetail from './components/ExerciseDetail';
import './App.css';

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
  const [panelWidths, setPanelWidths] = useState<{ left: number; right: number }>({ left: 50, right: 50 });
  const [dragging, setDragging] = useState(false);

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

  // Drag handlers for resizable panels
  const handleDragStart = (e: React.MouseEvent<HTMLDivElement>) => {
    setDragging(true);
    document.body.style.cursor = 'col-resize';
  };
  const handleDrag = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!dragging) return;
    const totalWidth = window.innerWidth;
    const left = Math.max(20, Math.min(80, (e.clientX / totalWidth) * 100));
    setPanelWidths({ left, right: 100 - left });
  };
  const handleDragEnd = () => {
    setDragging(false);
    document.body.style.cursor = '';
  };
  React.useEffect(() => {
    if (!dragging) return;
    const move = (e: MouseEvent) => handleDrag(e as any);
    const up = () => handleDragEnd();
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', up);
    return () => {
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', up);
    };
  }, [dragging]);

  return (
    <div className="App" style={{ minHeight: '100vh', width: '100vw', margin: 0, padding: 0, boxSizing: 'border-box', display: 'flex', flexDirection: 'column', alignItems: 'stretch' }}>
      <header className="App-header" style={{ backgroundColor: '#282c34', padding: '20px 0', color: 'white', textAlign: 'center', width: '100%', margin: 0, boxSizing: 'border-box', alignSelf: 'stretch' }}>
        <h1 style={{ margin: 0 }}>SQL Practice Platform</h1>
      </header>
      <main style={{ flex: 1, width: '100%', margin: 0, padding: 0, display: 'flex', flexDirection: 'row', alignItems: 'stretch', justifyContent: 'stretch', gap: 0, minHeight: 0 }}>
        {selectedExercise ? (
          <>
            <div style={{ flexBasis: `${panelWidths.left}%`, flexGrow: 0, flexShrink: 0, minWidth: 0, height: '100%' }}>
              {/* Remove all maxWidth or centering from ExerciseDetail's container */}
              <ExerciseDetail
                exercise={selectedExercise}
                onBackToList={handleBackToList}
                onRunQuery={handleRunQuery}
              />
            </div>
            <div
              style={{ width: 6, cursor: 'col-resize', background: '#eee', zIndex: 10 }}
              onMouseDown={handleDragStart}
            />
            <div style={{ flexBasis: `${panelWidths.right}%`, flexGrow: 1, flexShrink: 0, minWidth: 0, height: '100%' }}>
              {/* The right panel is the code editor and results, already inside ExerciseDetail */}
            </div>
          </>
        ) : (
          <div style={{ width: '100%', maxWidth: 1000, margin: '0 auto' }}>
            <ExerciseList onSelectExercise={handleSelectExercise} />
          </div>
        )}
      </main>
      <footer style={{ textAlign: 'center', padding: '20px', marginTop: '30px', borderTop: '1px solid #eee', color: '#777' }}>
        <p>&copy; {new Date().getFullYear()} SQL Practice Platform</p>
      </footer>
    </div>
  );
};

export default App;
