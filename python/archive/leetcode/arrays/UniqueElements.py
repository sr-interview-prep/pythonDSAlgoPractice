# find the no. of unique elements in a sorted list
"""
Algo
two pointer approach
i, j
loop through j and when ith and jth element are not same:
increment i and replace ith element with jth element
this to ensure that the next of jth elements don't have dupes
"""


class UniqueElements:
    @staticmethod
    def execute(nums: list):
        if not nums:
            return []
        result = [nums[0]]  # Initialize result with the first element
        for i in range(1, len(nums)):
            if nums[i] != nums[i - 1]:  # Compare current element with the previous one
                result.append(nums[i])  # Append to result if it's different
        return result
