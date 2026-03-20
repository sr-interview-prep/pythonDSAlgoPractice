"""Remove a particular element and count the remaining elements in the list"""
from typing import List


class RemoveElementRemainingCount:
    def __init__(self, nums: List, val: int):
        self.nums = nums
        self.val = val

    def remove_element_remaining_count(self):
        i = 0
        for j in range(len(self.nums)):
            if self.nums[j] != self.val:
                self.nums[i] = self.nums[j]
                i += 1
        return i
