import React, { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import { Editor, OnMount } from '@monaco-editor/react';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import type monaco from 'monaco-editor';
import { format } from 'sql-formatter-plus';
import QueryResultTable from './QueryResultTable'; // Import the new table component

export interface Exercise { // Added export
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
    borderRadius: '5px', display: 'flex', flexDirection: 'column', height: 'calc(100vh - 120px)' // Adjusted height
  };
  const panelStyle: React.CSSProperties = { // Style for Panels
    border: '1px solid #eee', padding: '10px', backgroundColor: '#f9f9f9',
    borderRadius: '4px', height: '100%', display: 'flex', flexDirection: 'column', overflow: 'auto'
  };
  const editorWrapperStyle: React.CSSProperties = {
    flexGrow: 1, border: '1px solid #ccc', borderRadius: '4px', overflow: 'hidden', height: 'calc(100% - 70px)' // Adjust height based on surrounding elements
  };
  const resizeHandleStyle: React.CSSProperties = { // Basic style for resize handles
    backgroundColor: '#ddd',
    // width/height will be set based on direction
  };
  const verticalResizeHandleStyle: React.CSSProperties = {
    ...resizeHandleStyle,
    height: '8px',
    cursor: 'row-resize',
  };
  const horizontalResizeHandleStyle: React.CSSProperties = {
    ...resizeHandleStyle,
    width: '8px',
    cursor: 'col-resize',
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

      <h2 style={{ marginTop: 0, marginBottom: '10px', flexShrink: 0 }}>{exercise.name}</h2>

      <PanelGroup direction="vertical" style={{ flexGrow: 1, overflow: 'hidden' }}>
        <Panel defaultSize={60} minSize={20}>
          <PanelGroup direction="horizontal" style={{ height: '100%', width: '100%' }}>
            <Panel defaultSize={50} minSize={20} style={panelStyle}>
              <h3 style={{ marginTop: 0, marginBottom: '10px', flexShrink: 0 }}>Problem Description</h3>
              <div style={{ flexGrow: 1, overflowY: 'auto' }}>
                {exercise.problem_description ? (
                  <ReactMarkdown>{exercise.problem_description}</ReactMarkdown>
                ) : (
                  <p>No problem description available.</p>
                )}
              </div>
            </Panel>
            <PanelResizeHandle style={horizontalResizeHandleStyle} />
            <Panel defaultSize={50} minSize={20} style={panelStyle}>
              <h3 style={{ marginTop: 0, marginBottom: '5px', flexShrink: 0 }}>Your SQL Query</h3>
              <p style={{ fontSize: '0.9em', color: '#555', marginTop: '0', marginBottom: '10px', flexShrink: 0 }}>
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
                  onMount={handleEditorDidMount}
                  options={{
                    minimap: { enabled: true },
                    scrollBeyondLastLine: false,
                    fontSize: 14,
                    wordWrap: 'on',
                    automaticLayout: true,
                    folding: true,
                    showFoldingControls: 'mouseover',
                    multiCursorModifier: 'alt',
                  }}
                />
              </div>
              <p style={{ fontSize: '0.8em', color: '#666', marginTop: '5px', textAlign: 'center', flexShrink: 0 }}>
                Tip: Editor supports multi-cursor (Alt+Click), search (Ctrl+F), folding, and more.
              </p>
              <div style={{ ...buttonContainerStyle, flexShrink: 0 }}>
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
            </Panel>
          </PanelGroup>
        </Panel>
        <PanelResizeHandle style={verticalResizeHandleStyle} />
        <Panel defaultSize={40} minSize={10} style={{ ...panelStyle, marginTop: '0px' }}>
          {queryResult ? (
            <>
              <h3 style={{ marginTop: 0, marginBottom: '10px', flexShrink: 0 }}>Query Result</h3>
              <div style={{ flexGrow: 1, overflowY: 'auto' }}>
                {queryResult.error && <pre style={{ color: 'red', whiteSpace: 'pre-wrap', backgroundColor: '#ffebee', padding: '10px', borderRadius: '4px' }}>Error: {queryResult.error}</pre>}
                {queryResult.message && <p style={{ color: 'blue' }}>Message: {queryResult.message}</p>}
                {queryResult.columns && queryResult.rows && !queryResult.error &&
                  <QueryResultTable columnNames={queryResult.columns} data={queryResult.rows} />
                }
              </div>
            </>
          ) : (
            <div style={{textAlign: 'center', color: '#888', marginTop: '20px'}}>
                Run a query to see the results.
            </div>
          )}
        </Panel>
      </PanelGroup>
    </div>
  );
};

export default ExerciseDetail;
