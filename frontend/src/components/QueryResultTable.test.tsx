import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import QueryResultTable from './QueryResultTable';

// Sample data for testing
const sampleColumnNames = ['ID', 'Name', 'Category', 'Stock', 'Status'];
const sampleData = [
  [1, 'Apple', 'Fruit', 10, 'Available'],
  [2, 'Banana', 'Fruit', 0, 'Out of Stock'],
  [3, 'Carrot', 'Vegetable', 5, 'Available'],
  [4, 'Date', null, 20, 'Available'], // Null category for NULL testing
  [5, 'Eggplant', 'Vegetable', 0, null], // Null status for NULL testing
  [6, 'Fig', 'Fruit', 15, 'Available'],
  [7, 'Grape', 'Fruit', null, 'Available'], // Null stock for NULL testing
];

import { act } from 'react'; // Import act

// Helper to get table cells' text content for a specific column by header text
const getColumnCells = (headerText: string): string[] => {
    const headerCell = screen.getByRole('columnheader', { name: headerText });
    if (!headerCell) throw new Error(`Header cell with text "${headerText}" not found.`);

    const headerIndex = Array.from(headerCell.parentElement!.children).indexOf(headerCell);

    const tableBody = screen.getByRole('table').querySelector('tbody');
    if (!tableBody) throw new Error('Table body not found');

    const dataRows = within(tableBody).getAllByRole('row');

    return dataRows.map(row => {
      const cell = row.children[headerIndex] as HTMLElement;
      return cell.textContent || '';
    });
};


describe('QueryResultTable Component', () => {
  beforeEach(async () => {
    // Wrap initial render in act if it causes state updates (though QueryResultTable might be simple enough)
    await act(async () => {
      render(<QueryResultTable columnNames={sampleColumnNames} data={sampleData} />);
    });
  });

  test('renders table with correct headers and initial data', () => {
    sampleColumnNames.forEach(headerText => {
      expect(screen.getByRole('columnheader', { name: headerText })).toBeInTheDocument();
    });
    // Use a more robust way to check for cell content, scoped to table body
    const tableBody = screen.getByRole('table').querySelector('tbody');
    expect(within(tableBody!).getByText('Apple')).toBeInTheDocument();
    expect(within(tableBody!).getByText('Carrot')).toBeInTheDocument();
    expect(within(tableBody!).getAllByText('NULL').length).toBeGreaterThan(0);
  });

  describe('Column-Level Filters (Heuristic-based: Name=dropdown, Category=dropdown, Stock=dropdown)', () => {
    // Adjusted expectation: Name column might be a dropdown due to heuristic
    test('filters by "Name" column (expecting dropdown) for "Apple"', async () => {
      const nameHeader = screen.getByRole('columnheader', { name: 'Name' });
      const filterSelect = within(nameHeader).getByRole('combobox');

      await act(async () => {
        fireEvent.change(filterSelect, { target: { value: 'Apple' } });
      });

      const nameCells = getColumnCells('Name');
      nameCells.forEach(cellText => expect(cellText).toBe('Apple'));
      expect(screen.queryByText('Banana')).not.toBeInTheDocument();
    });

    test('filters by "Category" column for "Fruit" (dropdown)', async () => {
      const categoryHeader = screen.getByRole('columnheader', { name: 'Category' });
      const filterSelect = within(categoryHeader).getByRole('combobox');

      await act(async () => {
        fireEvent.change(filterSelect, { target: { value: 'Fruit' } });
      });

      const categoryCells = getColumnCells('Category');
      categoryCells.forEach(cellText => expect(cellText).toBe('Fruit'));
      const tableBody = screen.getByRole('table').querySelector('tbody');
      expect(within(tableBody!).queryByText('Carrot')).not.toBeInTheDocument();
    });

    test('filters by "Category" column for NULL', async () => {
      const categoryHeader = screen.getByRole('columnheader', { name: 'Category' });
      const filterSelect = within(categoryHeader).getByRole('combobox');

      await act(async () => {
        fireEvent.change(filterSelect, { target: { value: '__NULL__' } });
      });
      const categoryCells = getColumnCells('Category');
      categoryCells.forEach(cellText => expect(cellText).toBe('NULL'));
      const tableBody = screen.getByRole('table').querySelector('tbody');
      expect(within(tableBody!).getByText('Date')).toBeInTheDocument();
      expect(within(tableBody!).queryByText('Apple')).not.toBeInTheDocument();
    });

    test('filters by "Category" column for NOT NULL', async () => {
      const categoryHeader = screen.getByRole('columnheader', { name: 'Category' });
      const filterSelect = within(categoryHeader).getByRole('combobox');

      await act(async () => {
        fireEvent.change(filterSelect, { target: { value: '__NOT_NULL__' } });
      });
      const categoryCells = getColumnCells('Category');
      categoryCells.forEach(cellText => expect(cellText).not.toBe('NULL'));
      const tableBody = screen.getByRole('table').querySelector('tbody');
      expect(within(tableBody!).queryByText('Date')).not.toBeInTheDocument();
      expect(within(tableBody!).getByText('Apple')).toBeInTheDocument();
    });
  });

  describe('Interaction with Global Filter', () => {
    test('global filter and column filter work together', async () => {
      const globalFilterInput = screen.getByPlaceholderText('Search all columns...');
      await act(async () => {
        fireEvent.change(globalFilterInput, { target: { value: 'Fruit' } });
      });

      const nameHeader = screen.getByRole('columnheader', { name: 'Name' });
      // Assuming Name is a dropdown after heuristic
      const nameFilterSelect = within(nameHeader).getByRole('combobox');
      await act(async () => {
        fireEvent.change(nameFilterSelect, { target: { value: 'Apple' } });
      });

      const tableBody = screen.getByRole('table').querySelector('tbody');
      expect(within(tableBody!).getByText('Apple')).toBeInTheDocument();
      expect(within(tableBody!).queryByText('Banana')).not.toBeInTheDocument();
      expect(within(tableBody!).queryByText('Fig')).not.toBeInTheDocument();
      expect(within(tableBody!).queryByText('Carrot')).not.toBeInTheDocument();
    });
  });

  test('filters by "Stock" column for a specific value (e.g., 10)', async () => {
    const stockHeader = screen.getByRole('columnheader', { name: 'Stock' });
    const filterSelect = within(stockHeader).getByRole('combobox');

    await act(async () => {
      fireEvent.change(filterSelect, { target: { value: '10' } });
    });

    const stockCells = getColumnCells('Stock');
    stockCells.forEach(cellText => expect(cellText).toBe('10'));
    const tableBody = screen.getByRole('table').querySelector('tbody');
    expect(within(tableBody!).getByText('Apple')).toBeInTheDocument();
    expect(within(tableBody!).queryByText('Banana')).not.toBeInTheDocument();
  });

  test('filters by "Stock" column for NULL values', async () => {
    const stockHeader = screen.getByRole('columnheader', { name: 'Stock' });
    const filterSelect = within(stockHeader).getByRole('combobox');

    await act(async () => {
      fireEvent.change(filterSelect, { target: { value: '__NULL__' } });
    });

    const stockCells = getColumnCells('Stock');
    stockCells.forEach(cellText => expect(cellText).toBe('NULL'));
    const tableBody = screen.getByRole('table').querySelector('tbody');
    expect(within(tableBody!).getByText('Grape')).toBeInTheDocument();
    expect(within(tableBody!).queryByText('Apple')).not.toBeInTheDocument();
  });
});
