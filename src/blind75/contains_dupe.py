def is_contains_dupe(nums):
    has_map = set()
    for num in nums:
        if num not in has_map:
            has_map.add(num)            
        else:
            return True
    return False


if __name__ == "__main__":
    # Test cases for is_contains_dupe
    assert is_contains_dupe([1, 2, 3, 4, 5]) == False, "Test case 1 failed"
    assert is_contains_dupe([1, 2, 3, 4, 5, 1]) == True, "Test case 2 failed"
    assert is_contains_dupe([]) == False, "Test case 3 failed"
    assert is_contains_dupe([1]) == False, "Test case 4 failed"
    assert is_contains_dupe([1, 2, 3, 2, 4, 5, 1]) == True, "Test case 5 failed"
    print("All test cases passed")
