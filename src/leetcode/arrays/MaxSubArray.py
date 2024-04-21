"""
Given an integer array nums, find the contiguous subarray (containing at least one number) which has the largest sum and return its sum.
Example 1:

Input: nums = [-2,1,-3,4,-1,2,1,-5,4]
Output: 6
Explanation: [4,-1,2,1] has the largest sum = 6.
Example 2:

Input: nums = [1]
Output: 1
"""


class MaxSubArray:
    def __init__(self, nums):
        self.nums = nums

    def get_max_sub_array(self):
        if len(self.nums) == 0:
            return 0
        max_sum = current_sum = self.nums[0]
        for i in self.nums[1:]:
            current_sum = max(i, current_sum + i)
            max_sum = max(max_sum, current_sum)
        return max_sum

# print(maxSubArray([-2,1,-3,4,-1,2,1,-5,4]))
