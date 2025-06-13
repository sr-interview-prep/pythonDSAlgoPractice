import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  test: { // Vitest configuration
    globals: true, // Use Vitest global APIs (describe, test, expect, etc.)
    environment: 'jsdom', // Simulate a browser environment for tests
    setupFiles: './src/setupTests.ts', // Path to your setup file for extending expect, etc.
    // Optionally, add coverage configuration if needed later
    // coverage: {
    //   reporter: ['text', 'json', 'html'],
    // }
  },
})
