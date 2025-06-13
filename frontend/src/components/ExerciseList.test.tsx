// frontend/src/components/ExerciseList.test.tsx
import { render, screen, waitFor } from '@testing-library/react';
import ExerciseList from './ExerciseList'; // Path to your ExerciseList component
import { describe, it, expect, vi } from 'vitest';
import axios from 'axios'; // To be mocked

// Mock axios
vi.mock('axios');
// For Vitest, vi.mocked provides type safety for mocked modules
const mockedAxios = vi.mocked(axios, true); // `true` for deep mocking if needed, or just vi.mocked(axios)

// Define the Exercise type, matching the component's expectation
interface Exercise {
  name: string;
  problem_description: string | null;
  solution_sql: string | null;
}

describe('ExerciseList Component', () => {
  it('renders loading state initially', () => {
    // Mock a pending promise state for initial render if useEffect runs immediately
    // For this test, we just want to see "Loading..." before any async ops complete.
    // The key is that the fetch operation hasn't resolved yet.
    mockedAxios.get.mockReturnValueOnce(new Promise(() => {})); // A promise that never resolves for this specific test
    render(<ExerciseList onSelectExercise={() => {}} />);
    expect(screen.getByText(/Loading exercises.../i)).toBeInTheDocument();
  });

  it('renders a list of exercises after successful fetch', async () => {
    const mockExercises: Exercise[] = [
      { name: 'Exercise 1', problem_description: 'Desc 1', solution_sql: 'SQL 1' },
      { name: 'Exercise 2', problem_description: 'Desc 2', solution_sql: 'SQL 2' },
    ];
    mockedAxios.get.mockResolvedValueOnce({ data: mockExercises });

    render(<ExerciseList onSelectExercise={() => {}} />);

    // Wait for loading to complete and exercises to be rendered
    await waitFor(() => {
      expect(screen.getByText(/Exercise 1/i)).toBeInTheDocument();
      expect(screen.getByText(/Exercise 2/i)).toBeInTheDocument();
    });
  });

  it('renders error message on fetch failure', async () => {
    mockedAxios.get.mockRejectedValueOnce(new Error('Network Error'));
    render(<ExerciseList onSelectExercise={() => {}} />);

    await waitFor(() => {
      // Check for part of the error message, as the full message might vary
      expect(screen.getByText(/Failed to fetch exercises/i)).toBeInTheDocument();
    });
  });

  it('renders "No exercises found." when fetch is successful but returns empty list', async () => {
    mockedAxios.get.mockResolvedValueOnce({ data: [] });
    render(<ExerciseList onSelectExercise={() => {}} />);

    await waitFor(() => {
        // The component shows a more specific message now.
        expect(screen.getByText(/No exercises found. Check if the backend is running and connected to the database./i)).toBeInTheDocument();
    });
  });
});
