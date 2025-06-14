import React, { useState, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import Editor from '@monaco-editor/react';

// Define the structure of an exercise object (can be shared or re-defined)
interface Exercise {
  name: string;
  problem_description: string | null;
  solution_sql: string | null; // This is part of the Exercise object
}

// Define props for the ExerciseDetail component
interface ExerciseDetailProps {
  exercise: Exercise;
  onBackToList: () => void; // Callback to go back to the exercise list
  // Callback to run query, returns a promise with the structure of QueryResult
  onRunQuery: (exerciseName: string, sqlQuery: string) => Promise<QueryResultData>;
}

// Define the structure for query results
interface QueryResultData {
  columns?: string[];
  rows?: any[][];
  error?: string;
  message?: string;
}

const ExerciseDetail: React.FC<ExerciseDetailProps> = ({ exercise, onBackToList, onRunQuery }) => {
  const [sqlQuery, setSqlQuery] = useState<string>('');
  const [queryResult, setQueryResult] = useState<QueryResultData | null>(null);
  const [isRunningQuery, setIsRunningQuery] = useState<boolean>(false);

  // Effect to pre-fill sqlQuery with solution_sql when exercise changes
  useEffect(() => {
    if (exercise && exercise.solution_sql) {
      setSqlQuery(exercise.solution_sql);
    } else {
      setSqlQuery(''); // Clear if no solution or exercise changes
    }
    setQueryResult(null); // Clear previous results when exercise changes
  }, [exercise]);

  const handleRunQuery = async () => {
    if (!sqlQuery.trim()) {
      setQueryResult({ error: "SQL query cannot be empty." });
      return;
    }
    setIsRunningQuery(true);
    setQueryResult(null); // Clear previous results before new query
    try {
      const result = await onRunQuery(exercise.name, sqlQuery);
      setQueryResult(result);
    } catch (e) {
      // This catch is for unexpected errors in the onRunQuery call itself or promise rejection
      setQueryResult({ error: "An unexpected error occurred while trying to run the query." });
      console.error("Error in onRunQuery promise or component handling:", e);
    } finally {
      setIsRunningQuery(false);
    }
  };

  return (
    <div style={{ border: '1px solid #ccc', padding: '15px', margin: '10px', borderRadius: '5px' }}>
      <button onClick={onBackToList} style={{ marginBottom: '15px', padding: '8px 12px' }}>
        &larr; Back to Exercise List
      </button>

      <h2>{exercise.name}</h2>

      <div style={{ border: '1px solid #eee', padding: '10px', marginBottom: '15px', backgroundColor: '#f9f9f9', borderRadius: '4px' }}>
        <h3>Problem Description</h3>
        {exercise.problem_description ? (
          <ReactMarkdown>{exercise.problem_description}</ReactMarkdown>
        ) : (
          <p>No problem description available.</p>
        )}
      </div>

      <div>
        <h3>Your SQL Query</h3>
        <p><em>(Solution SQL is pre-filled if available. You can modify it.)</em></p>
        <Editor
          height="300px"
          defaultLanguage="sql"
          value={sqlQuery}
          onChange={(value: string | undefined) => setSqlQuery(value || '')}
          options={{
            folding: true,
            formatOnType: true,
            formatOnPaste: true,
            minimap: { enabled: false },
            wordWrap: "on",
            fontSize: 16,
            fontFamily: 'monospace',
            scrollBeyondLastLine: false,
            automaticLayout: true,
          }}
        />
        <button
          onClick={handleRunQuery}
          disabled={isRunningQuery || !sqlQuery.trim()}
          style={{
            padding: '10px 15px',
            backgroundColor: isRunningQuery ? '#ccc' : '#28a745',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: isRunningQuery || !sqlQuery.trim() ? 'not-allowed' : 'pointer'
          }}
        >
          {isRunningQuery ? 'Running...' : 'Run Query'}
        </button>
      </div>

      {queryResult && (
        <div style={{ marginTop: '20px', border: '1px solid #ddd', padding: '10px', backgroundColor: '#fdfdfd', borderRadius: '4px' }}>
          <h3>Query Result</h3>
          {queryResult.error && <pre style={{ color: 'red', whiteSpace: 'pre-wrap', backgroundColor: '#ffebee', padding: '10px', borderRadius: '4px' }}>Error: {queryResult.error}</pre>}
          {queryResult.message && <p style={{ color: 'blue' }}>Message: {queryResult.message}</p>}
          {queryResult.columns && queryResult.rows && (
            <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr>
                    {queryResult.columns.map((col) => (
                      <th key={col} style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'left', backgroundColor: '#f0f0f0' }}>
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {queryResult.rows.length === 0 && (
                    <tr>
                      <td colSpan={queryResult.columns.length} style={{ textAlign: 'center', padding: '10px' }}>
                        Query executed successfully, but returned no rows.
                      </td>
                    </tr>
                  )}
                  {queryResult.rows.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      {row.map((cell, cellIndex) => (
                        <td key={cellIndex} style={{ border: '1px solid #ddd', padding: '8px' }}>
                          {String(cell === null ? "NULL" : cell)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default ExerciseDetail;
