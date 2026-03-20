import copy


class SetMatrixZero:
    def __init__(self, matrix):
        self.matrix = copy.deepcopy(matrix)
        # The deepcopy to ensure the original list is not getting updated
        self.no_rows = len(matrix)
        self.no_cols = len(matrix[0])

    def highlight_zero_rows(self, row):
        for col in range(self.no_cols):
            self.matrix[row][col] = '*'

    def highlight_zero_cols(self, col):
        for row in range(self.no_rows):
            self.matrix[row][col] = '*'

    def brute_force(self):
        for row in range(self.no_rows):
            for col in range(self.no_cols):
                if self.matrix[row][col] == 0:
                    self.highlight_zero_rows(row)
                    self.highlight_zero_cols(col)

        for row in range(self.no_rows):
            for col in range(self.no_cols):
                if self.matrix[row][col] == "*":
                    self.matrix[row][col] = 0

        return self.matrix

    def better_approach(self):
        row_array = [0] * self.no_rows
        col_array = [0] * self.no_cols
        for row in range(self.no_rows):
            for col in range(self.no_cols):
                if self.matrix[row][col] == 0:
                    row_array[row] = 1
                    col_array[col] = 1

        for row in range(self.no_rows):
            for col in range(self.no_cols):
                if row_array[row] or col_array[col]:
                    self.matrix[row][col] = 0

        return self.matrix


'''
Side note on when to use deepcopy:
------------------------------------------
Use deepcopy for nested mutable structures (lists of lists, dictionaries of lists, objects with mutable attributes).
Use shallow copy methods (slicing, copy(), tuple(), etc.) for flat structures or those containing only immutable elements.
------------------------------------------
'''
