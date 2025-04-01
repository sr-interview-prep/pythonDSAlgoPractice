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
    assert max_contiguous_sum([1, -2, 3, 4, -1, 2, 1, -5, 4]) == 9  # Expected output: 7
    assert max_contiguous_sum([5]) == 5  # Expected output: 5
    print("tests passed")
