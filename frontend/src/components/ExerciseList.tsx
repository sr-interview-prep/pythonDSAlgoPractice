import React, { useState, useEffect } from 'react'; // Corrected: useState, useEffect from 'react'
import axios from 'axios';

// Define the structure of an exercise object based on backend API response
interface Exercise {
  name: string;
  problem_description: string | null; // Assuming these can be null if file not found
  solution_sql: string | null;
}

// Define props for the ExerciseList component
interface ExerciseListProps {
  onSelectExercise: (exercise: Exercise) => void; // Callback to notify parent of selection
}

const ExerciseList: React.FC<ExerciseListProps> = ({ onSelectExercise }) => {
  const [exercises, setExercises] = useState<Exercise[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchExercises = async () => {
      setLoading(true);
      setError(null);
      try {
        // Backend API is running on http://localhost:8000 as per previous setup
        const response = await axios.get<Exercise[]>('http://localhost:8000/api/exercises');
        setExercises(response.data);
      } catch (err) {
        if (axios.isAxiosError(err)) {
          let errorMsg = `Failed to fetch exercises: ${err.message}.`;
          if (err.response) {
            errorMsg += ` Status: ${err.response.status} - ${err.response.statusText}.`;
            // Potentially log err.response.data if it contains useful info
          } else if (err.request) {
            errorMsg += ` No response received from server. Ensure the backend is running and accessible.`;
          }
          setError(errorMsg);
        } else {
          setError('An unexpected error occurred while fetching exercises.');
        }
        console.error("Error fetching exercises:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchExercises();
  }, []); // Empty dependency array means this effect runs once on mount

  if (loading) {
    return <p>Loading exercises...</p>;
  }

  if (error) {
    return <p style={{ color: 'red' }}>{error}</p>;
  }

  if (exercises.length === 0) {
    return <p>No exercises found. Check if the backend is running and connected to the database.</p>;
  }

  return (
    <div style={{
      border: '1px solid #ccc',
      padding: '15px',
      margin: '20px auto', // Changed margin for auto horizontal centering
      borderRadius: '5px',
      maxWidth: '600px',  // Added a max-width for the list container
      boxSizing: 'border-box' // Explicitly ensure box-sizing, though inherited
    }}>
      <h2 style={{ marginTop: '0' }}>Available Exercises</h2> {/* Added marginTop: 0 to h2 */}
      <ul style={{ listStyleType: 'none', padding: 0 }}>
        {exercises.map((exercise) => (
          <li key={exercise.name} style={{ marginBottom: '10px' }}>
            <button
              onClick={() => onSelectExercise(exercise)}
              style={{
                padding: '8px 15px',
                backgroundColor: '#007bff',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                width: '100%', // Make buttons take full width of li
                textAlign: 'left'
              }}
              onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#0056b3'}
              onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#007bff'}
            >
              {exercise.name}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
};

export default ExerciseList;
