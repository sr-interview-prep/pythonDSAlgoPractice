"""
Problem Statement:
Given an integer array nums, find the contiguous subarray (containing at least one number) which has the largest sum and return its sum.

Intuition:
- Use Kadane's Algorithm: As you iterate, keep track of the current subarray sum (current_sum).
- If adding the current element increases the sum, continue; otherwise, start a new subarray from the current element.
- Track the maximum sum found so far (max_sum).
- This approach efficiently finds the maximum subarray sum in O(n) time.

Time Complexity: O(n), where n is the number of elements in nums (single pass through the list)
Space Complexity: O(1), only a constant amount of extra space is used
"""

def max_contiguous_sum(nums):
    if not nums:
        return 0
    current_sum = max_sum = nums[0]
    for i in range(1, len(nums)):
        current_sum = max(nums[i], current_sum + nums[i])
        max_sum = max(current_sum, max_sum)
    return max_sum

if __name__ == "__main__":
    # Basic tests for contiguous sum
    assert max_contiguous_sum([1, 2, 3, 4]) == 10  # Expected output: 10
    assert max_contiguous_sum([-1, -2, -3, -4]) == -1  # Expected output: -1
    assert max_contiguous_sum([1, -2, 3, 4, -1, 2, 1, -5, 4]) == 10  # Expected output: 10
    assert max_contiguous_sum([5]) == 5  # Expected output: 5
    print("tests passed")
