import React from 'react';
import { render, screen } from '@testing-library/react';
import ExerciseDetail, { Exercise } from './ExerciseDetail'; // Assuming Exercise is exported or defined here
import '@testing-library/jest-dom';

// Manual mock for split-pane-react is in src/components/__mocks__/split-pane-react.tsx
// Jest should pick it up automatically.

// Mock Monaco Editor
jest.mock('@monaco-editor/react', () => ({
  __esModule: true,
  Editor: jest.fn((props) => (
    <textarea
      data-testid="monaco-editor"
      value={props.value}
      onChange={(e) => props.onChange && props.onChange(e.target.value, undefined)}
    />
  )),
}));


describe('ExerciseDetail Component', () => {
  const mockExercise: Exercise = {
    name: 'Test Exercise',
    problem_description: 'Solve this test problem.',
    solution_sql: 'SELECT * FROM test_table;',
  };

  const mockOnBackToList = jest.fn();
  const mockOnRunQuery = jest.fn();

  beforeEach(() => {
    // Clear mock calls before each test
    mockOnBackToList.mockClear();
    mockOnRunQuery.mockClear();
    // If mockSplitPane was imported from the manual mock, clear it:
    // import mockSplitPane from 'split-pane-react'; // This would now import the mock
    // if (jest.isMockFunction(mockSplitPane)) {
    //   mockSplitPane.mockClear();
    // }
  });

  test('renders SplitPane component for layout', () => {
    render(
      <ExerciseDetail
        exercise={mockExercise}
        onBackToList={mockOnBackToList}
        onRunQuery={mockOnRunQuery}
      />
    );

    const splitPaneElement = screen.getByTestId('split-pane');
    expect(splitPaneElement).toBeInTheDocument();
  });

  test('SplitPane receives correct "split" prop', () => {
    render(
      <ExerciseDetail
        exercise={mockExercise}
        onBackToList={mockOnBackToList}
        onRunQuery={mockOnRunQuery}
      />
    );

    const splitPaneElement = screen.getByTestId('split-pane');
    expect(splitPaneElement).toHaveAttribute('data-split', 'vertical');
  });

  test('renders problem description and SQL editor areas', () => {
    render(
      <ExerciseDetail
        exercise={mockExercise}
        onBackToList={mockOnBackToList}
        onRunQuery={mockOnRunQuery}
      />
    );
    expect(screen.getByText('Problem Description')).toBeInTheDocument();
    expect(screen.getByText('Your SQL Query')).toBeInTheDocument();
    // Check if the editor mock is rendered
    expect(screen.getByTestId('monaco-editor')).toBeInTheDocument();
  });

   test('displays exercise name and problem description', () => {
    render(
        <ExerciseDetail
            exercise={mockExercise}
            onBackToList={mockOnBackToList}
            onRunQuery={mockOnRunQuery}
        />
    );
    expect(screen.getByText(mockExercise.name)).toBeInTheDocument();
    expect(screen.getByText(/Solve this test problem./)).toBeInTheDocument(); // Using regex for partial match
  });

  test('pre-fills SQL query from exercise solution', () => {
    render(
        <ExerciseDetail
            exercise={mockExercise}
            onBackToList={mockOnBackToList}
            onRunQuery={mockOnRunQuery}
        />
    );
    const editor = screen.getByTestId('monaco-editor');
    expect(editor).toHaveValue(mockExercise.solution_sql);
  });

  // More tests can be added for button interactions, etc.
});
