def get_min_rotated_sorted_array(nums):
    """
    Algo
    Binary search
    If mid < right:
     - This is normal. Right portion is sorted
     - The mid could be the minimum or be any given value
        - In this case we search for the min on the left array by making the right index = mid
    If mid > right:
        The min element must be between mid and right
        - left = mid + 1
    At last the element at the left is the min value
    """
    left, right = 0, len(nums) - 1
    while left < right:
        mid = (left + right) // 2
        if nums[mid] < nums[right]:
            right = mid
        else:
            left = mid + 1
    return nums[left]


if __name__ == "__main__":
    # Test cases for get_min_rotated_sorted_array
    assert get_min_rotated_sorted_array([3, 4, 5, 6, 7, 1, 2]) == 1, "Test case 1 failed"
    assert get_min_rotated_sorted_array([4, 5, 6, 7, 0, 1, 2]) == 0, "Test case 2 failed"
    assert get_min_rotated_sorted_array([1, 2, 3, 4, 5, 6, 7]) == 1, "Test case 3 failed"
    assert get_min_rotated_sorted_array([2, 1]) == 1, "Test case 4 failed"
    assert get_min_rotated_sorted_array([1]) == 1, "Test case 5 failed"
    print("All test cases passed")
