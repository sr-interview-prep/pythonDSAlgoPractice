"""
Problem Statement:
Given an array of integers, find the pair of elements with the smallest absolute difference between them.
Return the pair as a tuple (smaller, larger).
If there are multiple pairs with the same minimum difference, return the first such pair found after sorting.

Intuition:
- The closest pair will always be adjacent after sorting the array, because sorting arranges numbers in increasing order and minimizes the difference between neighbors.
- By comparing only consecutive elements, we avoid unnecessary comparisons and achieve an efficient solution.
"""

def closest_pair(arr):
    # Sort the array to bring closest numbers next to each other
    arr.sort()
    min_diff = float('inf')  # Initialize minimum difference to infinity
    result = ()  # To store the closest pair
    for i in range(1, len(arr)):
        diff = arr[i] - arr[i - 1]  # Difference between consecutive elements
        if diff < min_diff:
            min_diff = diff         # Update minimum difference
            result = (arr[i - 1], arr[i])  # Update result with the new closest pair
    return result

if __name__ == "__main__":
    # Test cases with assertions
    assert closest_pair([6, 2, 4, 10]) == (2, 4)
    assert closest_pair([1, 5, 3, 19, 18, 25]) == (18, 19)
    assert closest_pair([30, 5, 20, 9]) == (5, 9)
    assert closest_pair([1, 1, 1, 1]) == (1, 1)
    assert closest_pair([-10, -20, -30, -40]) == (-40, -30)
    assert closest_pair([100]) == ()
    assert closest_pair([]) == ()
    print("All test cases passed!")
