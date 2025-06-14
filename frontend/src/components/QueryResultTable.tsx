import React, { useMemo, useState } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  getFacetedUniqueValues, // Added for unique value filtering
  flexRender,
  ColumnDef,
  SortingState,
  Column, // Added for ColumnFilter prop type
} from '@tanstack/react-table';

// Helper type for filter component props
interface ColumnFilterProps<T extends DataRow> {
  column: Column<T, unknown>;
}

// Enhanced Filter Component
const ColumnFilter = <T extends DataRow>({ column }: ColumnFilterProps<T>) => {
  const filterType = (column.columnDef.meta as any)?.filter || 'text'; // Default to text filter
  const columnFilterValue = column.getFilterValue();

  // For text input filter
  if (filterType === 'text') {
    return (
      <input
        type="text"
        value={(columnFilterValue ?? '') as string}
        onChange={e => column.setFilterValue(e.target.value)}
        placeholder={`Search ${column.id}...`}
        onClick={(e) => e.stopPropagation()}
        style={{ width: '90%', marginTop: '5px', padding: '4px', border: '1px solid #ccc', borderRadius: '3px' }}
      />
    );
  }

  // For dropdown filter (unique values, NULL, NOT NULL)
  if (filterType === 'dropdown') {
    const uniqueValues = useMemo(() => {
      const facetedValues = column.getFacetedUniqueValues();
      const sortedUniqueValues = Array.from(facetedValues.keys()).sort();
      return sortedUniqueValues;
    }, [column.getFacetedUniqueValues()]);

    return (
      <select
        value={(columnFilterValue ?? 'all') as string}
        onChange={e => {
          const value = e.target.value;
          if (value === 'all') {
            column.setFilterValue(undefined); // Clear filter
          } else {
            column.setFilterValue(value);
          }
        }}
        onClick={(e) => e.stopPropagation()}
        style={{ width: '95%', marginTop: '5px', padding: '4px', border: '1px solid #ccc', borderRadius: '3px' }}
      >
        <option value="all">All</option>
        <option value="__NULL__">NULL</option>
        <option value="__NOT_NULL__">NOT NULL</option>
        {uniqueValues.map(value => (
          <option key={String(value)} value={String(value === null ? "__NULL_FACETED__" : value)}>
            {value === null ? "NULL (from data)" : String(value)}
          </option>
        ))}
      </select>
    );
  }

  return null; // No filter for this column type
};


type DataRow = any[];

interface QueryResultTableProps {
  data: DataRow[];
  columnNames: string[];
}

const QueryResultTable: React.FC<QueryResultTableProps> = ({ data, columnNames }) => {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = useState<string>(''); // State for global filter

  const memoizedColumns = useMemo<ColumnDef<DataRow>[]>(() => {
    // Determine if a column is likely to have few unique values for dropdown suitability
    // This is a heuristic. A more robust approach might involve analyzing data types from backend.
    const isLikelyDropdown = (colIndex: number) => {
      if (data.length === 0) return false;
      const uniqueValues = new Set(data.map(row => row[colIndex]));
      // If more than 20 unique values or more than 1/3rd of rows are unique, prefer text.
      // Also, if values are very long, text is better.
      if (uniqueValues.size > 20 || uniqueValues.size > data.length / 3) {
          const firstValue = data[0][colIndex];
          if (typeof firstValue === 'string' && firstValue.length > 50) return false; // Avoid dropdowns for long text
          return uniqueValues.size <= 20; // Re-evaluate if it's just long text but few unique values
      }
      return true;
    };

    return columnNames.map((colName, index) => {
      const columnDataType = data.length > 0 ? typeof data[0][index] : 'string';
      const filterType = isLikelyDropdown(index) || columnDataType === 'boolean' ? 'dropdown' : 'text';

      return {
        accessorFn: (row) => row[index],
        id: colName,
        header: colName,
        cell: info => {
          const value = info.getValue();
          return String(value === null || value === undefined ? "NULL" : value);
        },
        filterFn: (row, columnId, filterValue) => {
          const rowValue = row.getValue(columnId);
          if (filterValue === '__NULL__' || filterValue === '__NULL_FACETED__') {
            return rowValue === null || rowValue === undefined;
          }
          if (filterValue === '__NOT_NULL__') {
            return rowValue !== null && rowValue !== undefined;
          }
          if (typeof rowValue === 'string') {
            return rowValue.toLowerCase().includes(String(filterValue).toLowerCase());
          }
          if (typeof rowValue === 'number' || typeof rowValue === 'boolean') {
            return String(rowValue).toLowerCase() === String(filterValue).toLowerCase();
          }
          return false; // Default for other types
        },
        meta: { filter: filterType }
      };
    });
  }, [columnNames, data]); // Added data dependency for heuristic

  const tableData = useMemo(() => data, [data]);

  const table = useReactTable({
    data: tableData,
    columns: memoizedColumns,
    state: {
      sorting,
      globalFilter, // Add globalFilter state
    },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter, // Add globalFilter change handler
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getFacetedUniqueValues: getFacetedUniqueValues(), // Enable faceted unique values
  });

  // If no columns (e.g. after non-SELECT query), render nothing from table component.
  if (columnNames.length === 0) {
      return null;
  }

  // If columns exist, but no data rows after filtering (or initially)
  const noDataAfterFilter = table.getRowModel().rows.length === 0;
  const noInitialData = columnNames.length > 0 && (!data || data.length === 0);

  return (
    <> {/* Fragment to hold filter input and table */}
      <div style={{ marginBottom: '10px', display: 'flex', alignItems: 'center', gap: '10px' }}>
        <input
          type="text"
          value={globalFilter}
          onChange={(e) => setGlobalFilter(e.target.value)}
          placeholder="Search all columns..."
          style={{ padding: '8px', border: '1px solid #ccc', borderRadius: '4px', flexGrow: 1, maxWidth: '400px' }}
        />
        <button
          onClick={() => {
            table.resetColumnFilters(true); // true to reset faceted values as well
            table.setGlobalFilter('');    // Clear global filter
          }}
          style={{
            padding: '8px 12px',
            border: '1px solid #ccc',
            borderRadius: '4px',
            backgroundColor: '#f0f0f0',
            cursor: 'pointer',
          }}
        >
          Reset Filters
        </button>
      </div>

      <div style={{ width: '100%', overflowX: 'auto', maxHeight: '400px', overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            {table.getHeaderGroups().map(headerGroup => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map(header => (
                  <th
                    key={header.id}
                    style={{
                        border: '1px solid #ddd',
                        padding: '8px',
                        textAlign: 'left',
                        backgroundColor: '#f0f0f0',
                        position: 'sticky', top: 0, zIndex: 1,
                        cursor: header.column.getCanSort() ? 'pointer' : 'default',
                        userSelect: 'none',
                        // verticalAlign: 'top' // Align header content top if filters make it too tall
                    }}
                  >
                    <div onClick={header.column.getToggleSortingHandler()} style={{ cursor: header.column.getCanSort() ? 'pointer' : 'default', display: 'inline-block' }}>
                      {header.isPlaceholder
                        ? null
                        : flexRender(
                            header.column.columnDef.header,
                            header.getContext()
                          )}
                      {{
                        asc: ' 🔼',
                        desc: ' 🔽',
                      }[header.column.getIsSorted() as string] ?? null}
                    </div>
                    {/* Render ColumnFilter if the column can be filtered */}
                    {header.column.getCanFilter() ? (
                      <div>
                        <ColumnFilter column={header.column as any} /> {/* Using 'as any' for now, will refine if needed */}
                      </div>
                    ) : null}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {noDataAfterFilter ? (
                 <tr>
                   <td colSpan={columnNames.length} style={{ textAlign: 'center', padding: '10px' }}>
                     {noInitialData ? "Query executed successfully, but returned no rows." : "No results found for your search."}
                   </td>
                 </tr>
            ) : (
              table.getRowModel().rows.map(row => (
                <tr key={row.id}>
                  {row.getVisibleCells().map(cell => (
                    <td key={cell.id} style={{ border: '1px solid #ddd', padding: '8px' }}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </>
  );
};

export default QueryResultTable;
