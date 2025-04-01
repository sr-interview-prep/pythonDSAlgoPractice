"""
Algorithm
1) Keep breaking the list in 2 until there are ind lists of ind elements of the original list
2) Take comb of two compare and make 2 element sorted list and repeat the process until final list
"""
'''O(n) is the space complexity as the list is broken into n chunks of lists
for breaking the list, it is log(n) 
for going through each element and combining it is n
Therefore, o(nlogn) is the time complexity'''


def merge(list1, list2):
    i = j = 0
    combined = []
    while i < len(list1) and j < len(list2):
        if list1[i] < list2[j]:
            combined.append(list1[i])
            i += 1
        else:
            combined.append(list2[j])
            j += 1
    while i < len(list1):
        combined.append(list1[i])
        i += 1
    while j < len(list2):
        combined.append(list2[j])
        j += 1
    return combined


def merge_sort(nums):
    if len(nums) == 0:
        return []
    if len(nums) == 1:
        return nums
    mid = len(nums) // 2
    left = nums[:mid]
    right = nums[mid:]
    return merge(merge_sort(left), merge_sort(right))


if __name__ == "__main__":
    # Test cases (input lists only)
    test_cases = [
        [],
        [1],
        [3, 2, 1],
        [5, 3, 8, 6, 2, 7],
        [10, -1, 2, 5, 0, 6, 4, -5],
        [1, 2, 3, 4, 5],  # Already sorted
        [5, 4, 3, 2, 1],  # Reverse sorted
        [1, 3, 2, 5, 4],  # Alternating
        [1, 1, 1, 1],  # All duplicates
        [2, 1, 1, 3, 2]  # With duplicates
    ]

    for i, input_list in enumerate(test_cases):
        expected = sorted(input_list)  # Compare with Python's built-in sorted
        result = merge_sort(input_list)
        assert result == expected, f"Test case {i + 1} failed: {input_list} got {result} expected {expected}"
    print("All test cases passed!")
