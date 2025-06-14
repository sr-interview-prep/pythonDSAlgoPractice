import React, { useMemo, useState } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel, // Added for filtering
  flexRender,
  ColumnDef,
  SortingState,
} from '@tanstack/react-table';

type DataRow = any[];

interface QueryResultTableProps {
  data: DataRow[];
  columnNames: string[];
}

const QueryResultTable: React.FC<QueryResultTableProps> = ({ data, columnNames }) => {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = useState<string>(''); // State for global filter

  const memoizedColumns = useMemo<ColumnDef<DataRow>[]>(() => {
    return columnNames.map((colName, index) => ({
      accessorFn: (row) => row[index],
      id: colName,
      header: colName,
      cell: info => String(info.getValue() === null ? "NULL" : info.getValue()),
    }));
  }, [columnNames]);

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
    getFilteredRowModel: getFilteredRowModel(), // Add filtered row model
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
      <div style={{ marginBottom: '10px' }}>
        <input
          type="text"
          value={globalFilter}
          onChange={(e) => setGlobalFilter(e.target.value)}
          placeholder="Search all columns..."
          style={{ padding: '8px', border: '1px solid #ccc', borderRadius: '4px', width: '100%', maxWidth: '400px' }}
        />
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
                    }}
                    onClick={header.column.getToggleSortingHandler()}
                  >
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
