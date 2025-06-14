import React, { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import { Editor, OnMount } from '@monaco-editor/react';
import SplitPane from 'split-pane-react';
import 'split-pane-react/esm/themes/default.css';
import type monaco from 'monaco-editor';
import { format } from 'sql-formatter-plus';
import QueryResultTable from './QueryResultTable'; // Import the new table component

interface Exercise {
  name: string;
  problem_description: string | null;
  solution_sql: string | null;
}

interface ExerciseDetailProps {
  exercise: Exercise;
  onBackToList: () => void;
  onRunQuery: (exerciseName: string, sqlQuery: string) => Promise<QueryResultData>;
}

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
  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null); // Ref for editor instance

  const handleEditorDidMount: OnMount = (editor, _monacoInstance) => {
    editorRef.current = editor;
  };

  useEffect(() => {
    if (exercise && exercise.solution_sql) {
      setSqlQuery(exercise.solution_sql);
    } else {
      setSqlQuery('');
    }
    setQueryResult(null);
  }, [exercise]);

  const handleRunQuery = async () => {
    if (!sqlQuery.trim()) {
      setQueryResult({ error: "SQL query cannot be empty." });
      return;
    }
    setIsRunningQuery(true);
    setQueryResult(null);
    try {
      const result = await onRunQuery(exercise.name, sqlQuery);
      setQueryResult(result);
    } catch (e) {
      setQueryResult({ error: "An unexpected error occurred while trying to run the query." });
      console.error("Error in onRunQuery promise or component handling:", e);
    } finally {
      setIsRunningQuery(false);
    }
  };

  const handleFormatSQL = () => {
    if (editorRef.current) {
      const currentValue = editorRef.current.getValue();
      if (currentValue) {
        try {
          const formattedValue = format(currentValue, { language: 'sql' });
          editorRef.current.setValue(formattedValue);
        } catch (error) {
          console.error("Error formatting SQL:", error);
          // Optionally, display an error to the user in the UI
          // For example, by setting a state variable and showing it near the editor
        }
      }
    }
  };

  // Styles
  const componentRootStyle: React.CSSProperties = {
    border: '1px solid #ccc', padding: '15px', margin: '10px auto',
    borderRadius: '5px', display: 'flex', flexDirection: 'column', height: 'calc(100vh - 200px)' // Adjusted for header/footer
  };
  // Remove layoutContainerStyle and columnStyle as SplitPane will handle layout
  const columnContentBoxStyle: React.CSSProperties = {
    border: '1px solid #eee', padding: '10px', backgroundColor: '#f9f9f9',
    borderRadius: '4px', height: '100%', display: 'flex', flexDirection: 'column', overflow: 'auto'
  };
  const editorWrapperStyle: React.CSSProperties = {
    flexGrow: 1, border: '1px solid #ccc', borderRadius: '4px', overflow: 'hidden'
  };
  const buttonContainerStyle: React.CSSProperties = {
    marginTop: '10px', display: 'flex', gap: '10px', alignItems: 'center' // Added alignItems
  };
  const baseButtonStyle: React.CSSProperties = { // Base style for buttons
    padding: '10px 15px', color: 'white', border: 'none',
    borderRadius: '4px', cursor: 'pointer'
  };
  const formatButtonStyle: React.CSSProperties = {
    ...baseButtonStyle, backgroundColor: '#6c757d'
  };
  const runButtonStyle: React.CSSProperties = {
    ...baseButtonStyle,
    backgroundColor: isRunningQuery || !sqlQuery.trim() ? '#ccc' : '#28a745',
    cursor: isRunningQuery || !sqlQuery.trim() ? 'not-allowed' : 'pointer'
  };

  return (
    <div style={componentRootStyle}>
      <button onClick={onBackToList} style={{ marginBottom: '15px', padding: '8px 12px' }}>
        &larr; Back to Exercise List
      </button>

      <h2 style={{ marginTop: 0, marginBottom: '20px', flexShrink: 0 }}>{exercise.name}</h2>

      <div style={{ flexGrow: 1 }}> {/* This div will contain the SplitPane and allow it to take available space */}
        <SplitPane
          split="vertical" // or "horizontal"
          sizes={[50, 50]} // Initial sizes of panes in percentage or pixels
        >
          {/* Problem Description Pane */}
          <div style={{ ...columnContentBoxStyle, marginRight: '5px' }}> {/* Added marginRight for gutter */}
            <h3 style={{marginTop: 0, marginBottom: '10px'}}>Problem Description</h3>
            <div style={{ flexGrow: 1, overflowY: 'auto' }}>
              {exercise.problem_description ? (
                <ReactMarkdown>{exercise.problem_description}</ReactMarkdown>
              ) : (
                <p>No problem description available.</p>
              )}
            </div>
          </div>

          {/* SQL Editor Pane */}
          <div style={{ ...columnContentBoxStyle, marginLeft: '5px' }}> {/* Added marginLeft for gutter */}
            <h3 style={{marginTop: 0, marginBottom: '5px'}}>Your SQL Query</h3>
            <p style={{fontSize: '0.9em', color: '#555', marginTop: '0', marginBottom: '10px'}}>
              <em>(Solution SQL is pre-filled if available. You can modify it.)</em>
            </p>
            <div style={editorWrapperStyle}>
              <Editor
                height="100%"
                width="100%"
                defaultLanguage="sql"
                theme="vs-dark"
                value={sqlQuery}
                onChange={(value) => setSqlQuery(value || '')}
                onMount={handleEditorDidMount} // Added onMount handler
                options={{
                  minimap: { enabled: true },
                  scrollBeyondLastLine: false,
                  fontSize: 14,
                  wordWrap: 'on',
                  automaticLayout: true,
                  folding: true, // Ensure folding is enabled
                  showFoldingControls: 'mouseover', // Show folding controls on mouseover
                  multiCursorModifier: 'alt', // Use Alt+Click for multicursor
                  // find: { addExtraSpaceOnTop: false } // Example, if specific find widget customization is needed
                }}
              />
            </div>
            <p style={{ fontSize: '0.8em', color: '#666', marginTop: '5px', textAlign: 'center' }}>
              Tip: Editor supports multi-cursor (Alt+Click), search (Ctrl+F), folding, and more.
            </p>
            <div style={buttonContainerStyle}>
              <button
                onClick={handleFormatSQL}
                style={formatButtonStyle}
                onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#5a6268'}
                onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#6c757d'}
              >
                Format SQL
              </button>
              <button
                onClick={handleRunQuery}
                disabled={isRunningQuery || !sqlQuery.trim()}
                style={runButtonStyle}
              >
                {isRunningQuery ? 'Running...' : 'Run Query'}
              </button>
            </div>
          </div>
        </SplitPane> {/* Correctly closing SplitPane */}
      </div> {/* Correctly closing the flexGrow div that wraps SplitPane */}

      {/* Query Result Section (remains below the SplitPane) */}
      {queryResult && (
        <div style={{ marginTop: '20px', border: '1px solid #ddd', padding: '10px', backgroundColor: '#fdfdfd', borderRadius: '4px', flexShrink: 0 }}>
          <h3 style={{marginTop: 0}}>Query Result</h3>
          {queryResult.error && <pre style={{ color: 'red', whiteSpace: 'pre-wrap', backgroundColor: '#ffebee', padding: '10px', borderRadius: '4px' }}>Error: {queryResult.error}</pre>}
          {queryResult.message && <p style={{ color: 'blue' }}>Message: {queryResult.message}</p>}

          {/* Render QueryResultTable if columns and rows exist and no error */}
          {queryResult.columns && queryResult.rows && !queryResult.error &&
            <QueryResultTable columnNames={queryResult.columns} data={queryResult.rows} />
          }
          {/* Message for no rows is handled by QueryResultTable now if columns are present */}
        </div>
      )}
    </div>
  );
};

export default ExerciseDetail;
