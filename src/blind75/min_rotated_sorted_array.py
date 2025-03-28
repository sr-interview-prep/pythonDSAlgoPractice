def get_min_rotated_sorted_array(nums):
    """
    Algo
    345,  6  ,712  -> Works
    123,  4  ,567  -> Works

    Get the sorted part of the array:
        if mid<max then left is sorted but not right
    Consider the sorted one as reference
        if 1st element of sorted array > last element of unsorted array:
            min value exist in the unsorted array (continue binary search in unsorted array)
        else:
            1st element of sorted array is the min element
    """
    left, right = 0, len(nums) - 1
    while left < right:
        mid = (left + right) // 2
        if nums[mid] > nums[right]:
            left = mid + 1
        else:
            right = mid
    return nums[left]


if __name__ == "__main__":
    # Test cases for get_min_rotated_sorted_array
    assert get_min_rotated_sorted_array([3, 4, 5, 6, 7, 1, 2]) == 1, "Test case 1 failed"
    assert get_min_rotated_sorted_array([4, 5, 6, 7, 0, 1, 2]) == 0, "Test case 2 failed"
    assert get_min_rotated_sorted_array([1, 2, 3, 4, 5, 6, 7]) == 1, "Test case 3 failed"
    assert get_min_rotated_sorted_array([2, 1]) == 1, "Test case 4 failed"
    assert get_min_rotated_sorted_array([1]) == 1, "Test case 5 failed"
    print("All test cases passed")
