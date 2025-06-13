// frontend/src/App.test.tsx
import { render, screen } from '@testing-library/react';
import App from './App'; // Path to your App component
import { describe, it, expect } from 'vitest';

describe('App Component', () => {
  it('renders the main header', () => {
    render(<App />);
    // Check if an element with the text "SQL Practice Platform" is present
    // Using a text matcher that is case-insensitive and can find partial matches within elements
    const headerElement = screen.getByText(/SQL Practice Platform/i);
    expect(headerElement).toBeInTheDocument(); // Basic assertion
  });
});
