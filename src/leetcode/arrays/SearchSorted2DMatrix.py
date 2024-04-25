"""Write an efficient algorithm that searches for a value target in an m x n integer matrix matrix. This matrix has the following properties:

Integers in each row are sorted from left to right.
The first integer of each row is greater than the last integer of the previous row.


Example 1:


Input: matrix = [[1,3,5,7],[10,11,16,20],[23,30,34,60]], target = 3
Output: true
Example 2:


Input: matrix = [[1,3,5,7],[10,11,16,20],[23,30,34,60]], target = 13
Output: false"""
from typing import List


class SearchSorted2DMatrix:
    @staticmethod
    def search_matrix(matrix: List[List[int]], target: int) -> bool:
        row = 0
        # finding the row where the target is present
        for i in range(len(matrix)):
            if target == matrix[i][len(matrix[i]) - 1]:
                return True
            elif target < matrix[i][len(matrix[i]) - 1]:
                row = i
                break
        # binary search in the row
        nums = matrix[row]
        left, right = 0, len(nums) - 1
        while left <= right:
            pivot = (left + right) // 2
            if nums[pivot] == target:
                return True
            if target < nums[pivot]:
                right = pivot - 1
            else:
                left = pivot + 1
        return False

    @staticmethod
    def search_matrix_alternate(matrix: List[List[int]], target: int) -> bool:
        def search(mat, inner_target):

            low = 0
            high = len(mat) - 1

            while low <= high:
                mid = (low + high) // 2

                if mat[mid] == inner_target:
                    return True

                elif inner_target < mat[mid]:
                    high = mid - 1

                elif inner_target > mat[mid]:
                    low = mid + 1

            return False

        for i in range(len(matrix)):
            if search(mat=matrix[i], inner_target=target):
                return True
        return False
