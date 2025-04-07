def max_profit(nums: list):
    if not nums:
        return -1, 0, 0, 0, 0

    min_val_index = 0
    max_val_index = 0

    min_value = nums[0]
    max_value = nums[0]

    cur_profit = 0
    max_profit = 0

    for i in range(1, len(nums)):
        cur_value = nums[i]

        cur_profit = cur_value - min_value
        if cur_profit > max_profit:
            max_value = cur_value
            max_val_index = i
            max_profit = cur_profit

        if cur_value < min_value:
            min_value = cur_value
            min_val_index = i

    return max_profit, min_val_index, max_val_index, min_value, max_value


if __name__ == "__main__":
    # Test cases
    test_cases = [
        ([7, 1, 5, 3, 6, 4], (5, 1, 4, 1, 6)),  # Example case
        ([7, 6, 4, 3, 1], (0, 4, 0, 1, 1)),  # No profit case
        ([1, 2, 3, 4, 5], (4, 0, 4, 1, 5)),  # Increasing prices
        ([5, 4, 3, 2, 1], (0, 4, 0, 1, 1)),  # Decreasing prices
        ([3, 3, 5, 0, 0, 3, 1, 4], (4, 3, 7, 0, 4)),  # Mixed prices
        ([], (-1, 0, 0, 0, 0)),  # Empty list
    ]

    for i, (input_data, expected) in enumerate(test_cases):
        result = max_profit(input_data)
        assert result == expected, f"Test case {i + 1} failed: expected {expected}, got {result}"
    print("All test cases passed!")
