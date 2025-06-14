import React from 'react';

const MockSplitPane = jest.fn(({ children, split }) => (
  <div data-testid="split-pane" data-split={split}>
    {React.Children.toArray(children)[0]} {/* Pane 1 */}
    <div data-testid="resizer" /> {/* Mock Resizer */}
    {React.Children.toArray(children)[1]} {/* Pane 2 */}
  </div>
));

export default MockSplitPane;
