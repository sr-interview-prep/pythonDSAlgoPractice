from typing import List


class Solution:
    def setZeroes(self, matrix: List[List[int]]) -> None:
        """
        Do not return anything, modify matrix in-place instead.
        """
        n = len(matrix)
        m = len(matrix[0])
        col0 = matrix[0][0]

        ## Whole Matrix
        for i in range(n):
            for j in range(m):
                if matrix[i][j] == 0:
                    # mark i-th row:
                    matrix[i][0] = 0

                    # mark j-th column:
                    if j != 0:
                        matrix[0][j] = 0
                    else:
                        col0 = 0

        ## The rectangle excluding the left and top crumbs
        for i in range(1, n):
            for j in range(1, m):
                if matrix[i][0] == 0 or matrix[0][j] == 0:
                    matrix[i][j] = 0

        ## updating the top crumb by columns and then left crumb by rows
        if matrix[0][0] == 0:
            for j in range(m):
                matrix[0][j] = 0

        ## finally updating the top left corner element
        if col0 == 0:
            for i in range(n):
                matrix[i][0] = 0

        return matrix


if __name__ == "__main__":
    input = [[1, 2, 3, 4], [5, 0, 7, 8], [0, 10, 11, 12], [13, 14, 15, 0]]
    result = Solution().setZeroes(matrix=input)
    assert True
