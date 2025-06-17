# Max Profit from Stock Prices - Single Pass Approach (returns only max profit)

def max_profit(prices: list):
    """
    Returns the maximum profit that can be achieved from a single buy and sell.
    If no profit is possible or input is empty, returns 0.
    """
    if not prices:
        return 0

    min_price = prices[0]
    max_profit = 0

    for price in prices[1:]:
        min_price=min(price, min_price)
        max_profit=max(max_profit, price-min_price)
    return max_profit

if __name__ == "__main__":
    # Test cases: (input, expected_output)
    test_cases = [
        ([7, 1, 5, 3, 6, 4], 5),
        ([7, 6, 4, 3, 1], 0),
        ([1, 2, 3, 4, 5], 4),
        ([5, 4, 3, 2, 1], 0),
        ([3, 3, 5, 0, 0, 3, 1, 4], 4),
        ([], 0),
    ]

    for i, (input_data, expected) in enumerate(test_cases):
        result = max_profit(input_data)
        assert result == expected, f"Test case {i + 1} failed: expected {expected}, got {result}"
    print("All test cases passed!")

'''
Time Complexity: O(n), where n is the number of prices (single pass through the list)
Space Complexity: O(1), only a constant amount of extra space is used
'''