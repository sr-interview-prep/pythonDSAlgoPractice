"""
Given an integer numRows, return the first numRows of Pascal's triangle.

In Pascal's triangle, each number is the sum of the two numbers directly above it as shown:




Example 1:

Input: numRows = 5
Output: [[1],[1,1],[1,2,1],[1,3,3,1],[1,4,6,4,1]]
Example 2:

Input: numRows = 1
Output: [[1]]
"""
from typing import List


class PascalsTriangle:
    def __init__(self, num_rows: int):
        self.num_rows = num_rows

    def generate(self) -> List[List[int]]:
        res = []
        cur_row = 1
        while cur_row <= self.num_rows:
            if cur_row == 1:
                res.append([1])
            elif cur_row == 2:
                res.append([1, 1])
            else:
                mid = [1]
                j = 0
                while j < cur_row - 2:
                    mid.append(res[-1][j] + res[-1][j + 1])
                    j += 1
                mid.append(1)
                res.append(mid)
            cur_row += 1
        return res
