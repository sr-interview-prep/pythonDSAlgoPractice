import React, { useState, useEffect, useMemo } from 'react';
import ReactMarkdown from 'react-markdown';
import Editor from '@monaco-editor/react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  ColumnDef,
  SortingState,
} from '@tanstack/react-table';

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

  // Table state for sorting/filtering
  const [globalFilter, setGlobalFilter] = useState('');
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnFilters, setColumnFilters] = useState<any[]>([]);

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

  const columns = useMemo<ColumnDef<any, any>[]>(() =>
    queryResult && queryResult.columns
      ? queryResult.columns.map((col) => ({
          accessorKey: col,
          header: col,
          cell: info => info.getValue(),
          enableSorting: true,
          enableColumnFilter: true,
        }))
      : [],
    [queryResult]
  );
  const data = useMemo(() => (queryResult && queryResult.rows && queryResult.columns ? queryResult.rows.map((row: any[]) => {
    const obj: Record<string, any> = {};
    queryResult.columns.forEach((col: string, i: number) => { obj[col] = row[i]; });
    return obj;
  }) : []), [queryResult]);

  const table = useReactTable({
    data,
    columns,
    state: { globalFilter, sorting, columnFilters },
    onGlobalFilterChange: setGlobalFilter,
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  });

  return (
    <div style={{ width: '100%', height: '100%', margin: 0, padding: 0, boxSizing: 'border-box', display: 'flex', flexDirection: 'column', flex: 1, minWidth: 0 }}>
      <button onClick={onBackToList} style={{ margin: '15px 0 0 15px', padding: '8px 12px', alignSelf: 'flex-start', position: 'absolute', zIndex: 2 }}>
        &larr; Back to Exercise List
      </button>
      <h2 style={{ margin: '30px 0 0 0', marginBottom: 0, textAlign: 'center' }}>{exercise.name}</h2>
      <div style={{ flex: 1, display: 'flex', flexDirection: 'row', width: '100%', height: '100%', overflow: 'hidden', marginTop: '10px', minWidth: 0 }}>
        {/* Left: Problem Description */}
        <div style={{ flex: 1, borderRight: '1px solid #eee', padding: '0 0', overflowY: 'auto', background: '#f9f9f9', minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'stretch', justifyContent: 'flex-start' }}>
          <div style={{ width: '100%', minWidth: 0, flex: 1, display: 'flex', flexDirection: 'column', maxWidth: '900px', margin: '0 auto' }}>
            <h3>Problem Description</h3>
            {exercise.problem_description ? (
              <ReactMarkdown>{exercise.problem_description}</ReactMarkdown>
            ) : (
              <p>No problem description available.</p>
            )}
          </div>
        </div>
        {/* Right: Code Editor and Query Result */}
        <div style={{ flex: 1.2, display: 'flex', flexDirection: 'column', padding: '16px 8px', overflow: 'hidden', minWidth: 0 }}>
          <h3>Your SQL Query</h3>
          <p><em>(Solution SQL is pre-filled if available. You can modify it.)</em></p>
          <div style={{ flex: 1, minHeight: 0, marginBottom: '12px', position: 'relative' }}>
            <Editor
              height="100%"
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
                contextmenu: true,
                quickSuggestions: true,
                suggestOnTriggerCharacters: true,
                tabCompletion: 'on',
                find: {
                  addExtraSpaceOnTop: false,
                  autoFindInSelection: 'always',
                },
              }}
              theme="vs-dark"
            />
          </div>
          <button
            onClick={handleRunQuery}
            disabled={isRunningQuery || !sqlQuery.trim()}
            style={{
              padding: '10px 15px',
              backgroundColor: isRunningQuery ? '#ccc' : '#28a745',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: isRunningQuery || !sqlQuery.trim() ? 'not-allowed' : 'pointer',
              marginBottom: '16px',
              alignSelf: 'flex-start'
            }}
          >
            {isRunningQuery ? 'Running...' : 'Run Query'}
          </button>
          {queryResult && (
            <div style={{ marginTop: '10px', border: '1px solid #ddd', padding: '10px', backgroundColor: '#fdfdfd', borderRadius: '4px', overflowY: 'auto', maxHeight: '40vh' }}>
              <h3>Query Result</h3>
              {queryResult.error && <pre style={{ color: 'red', whiteSpace: 'pre-wrap', backgroundColor: '#ffebee', padding: '10px', borderRadius: '4px' }}>Error: {queryResult.error}</pre>}
              {queryResult.message && <p style={{ color: 'blue' }}>Message: {queryResult.message}</p>}
              {queryResult.columns && queryResult.rows && (
                <>
                  <input
                    value={globalFilter}
                    onChange={e => setGlobalFilter(e.target.value)}
                    placeholder="Global filter..."
                    style={{ marginBottom: 8, padding: 4, width: '100%' }}
                  />
                  <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                      <thead>
                        {table.getHeaderGroups().map(headerGroup => (
                          <tr key={headerGroup.id}>
                            {headerGroup.headers.map(header => (
                              <th
                                key={header.id}
                                style={{ border: '1px solid #ddd', padding: '8px', textAlign: 'left', backgroundColor: '#f0f0f0', cursor: header.column.getCanSort() ? 'pointer' : undefined }}
                                onClick={header.column.getToggleSortingHandler()}
                              >
                                {flexRender(header.column.columnDef.header, header.getContext())}
                                {header.column.getIsSorted() ? (header.column.getIsSorted() === 'asc' ? ' ▲' : ' ▼') : ''}
                                <div>
                                  {header.column.getCanFilter() ? (
                                    <input
                                      type="text"
                                      value={(header.column.getFilterValue() ?? '') as string}
                                      onChange={e => header.column.setFilterValue(e.target.value)}
                                      placeholder={`Filter ${header.column.id}`}
                                      style={{ width: '90%', marginTop: 4, fontSize: 12 }}
                                    />
                                  ) : null}
                                </div>
                              </th>
                            ))}
                          </tr>
                        ))}
                      </thead>
                      <tbody>
                        {table.getRowModel().rows.length === 0 && (
                          <tr>
                            <td colSpan={columns.length} style={{ textAlign: 'center', padding: '10px' }}>
                              Query executed successfully, but returned no rows.
                            </td>
                          </tr>
                        )}
                        {table.getRowModel().rows.map(row => (
                          <tr key={row.id}>
                            {row.getVisibleCells().map(cell => (
                              <td key={cell.id} style={{ border: '1px solid #ddd', padding: '8px' }}>
                                {flexRender(cell.column.columnDef.cell, cell.getContext())}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ExerciseDetail;
