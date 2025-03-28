def is_contains_dupe(nums):
    hash_map = {}
    for i in range(len(nums)):
        if nums[i] in hash_map:
            return True
        else:
            hash_map[nums[i]] = i
    return False


if __name__ == "__main__":
    # Test cases for is_contains_dupe
    assert is_contains_dupe([1, 2, 3, 4, 5]) == False, "Test case 1 failed"
    assert is_contains_dupe([1, 2, 3, 4, 5, 1]) == True, "Test case 2 failed"
    assert is_contains_dupe([]) == False, "Test case 3 failed"
    assert is_contains_dupe([1]) == False, "Test case 4 failed"
    assert is_contains_dupe([1, 2, 3, 2, 4, 5, 1]) == True, "Test case 5 failed"
    print("All test cases passed")
