"""
Given an integer array nums, move all 0's to the end of it while maintaining the relative order of the non-zero elements.

Note that you must do this in-place without making a copy of the array.



Example 1:

Input: nums = [0,1,0,3,12]
Output: [1,3,12,0,0]

Dry Run:
l=r=0
[0,1,0,3,12]
l=0,r=1  => this means increment r all the time and increment l when ls[r]!=0 and swap l and r
[1,0,0,3,12]
l=1,r=2
[1,0,0,3,12]
l=1,r=3
[1,3,0,0,12]
l=2,r=4
[1,3,12,0,0]




Example 2:

Input: nums = [0]
Output: [0]

"""
from typing import List


class MoveZeros:
    def __init__(self, nums: List[int]):
        self.nums = nums

    def move_zeros(self) -> List[int]:
        """
        Do not return anything, modify nums in-place instead.
        """
        p = 0
        q = 0
        for i in range(len(self.nums)):
            if self.nums[i] == 0:
                q += 1
            else:
                self.nums[p], self.nums[q] = self.nums[q], self.nums[p]
                p += 1
                q += 1
        return self.nums

    def move_zeros(self) -> List[int]:
        """
        Do not return anything, modify nums in-place instead.
        """
        l = 0
        for r in range(len(self.nums)):
            if self.nums[r] != 0:
                self.nums[l], self.nums[r] = self.nums[r], self.nums[l]
                l += 1
        return self.nums
