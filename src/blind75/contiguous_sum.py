def contiguous_sum(nums):
    if not nums:
        return 0
    current_max = global_max = nums[0]
    for i in range(1, len(nums)):
        current_max = max(nums[i], current_max + nums[i])
        global_max = max(current_max, global_max)
    return global_max


if __name__ == "__main__":
    # Basic tests for contiguous sum
    assert contiguous_sum([1, 2, 3, 4]) == 10  # Expected output: 10
    assert contiguous_sum([-1, -2, -3, -4]) == -1  # Expected output: -1
    assert contiguous_sum([1, -2, 3, 4, -1, 2, 1, -5, 4]) == 9  # Expected output: 7
    assert contiguous_sum([5]) == 5  # Expected output: 5
    print("tests passed")
