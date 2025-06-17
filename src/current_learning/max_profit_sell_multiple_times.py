# Max Profit from Stock Prices - Multiple Transactions Allowed

def max_profit_sell_multiple_times(prices: list):
    """
    Returns the maximum profit that can be achieved by making as many buy/sell transactions as desired.
    (You must sell before you buy again.)
    If input is empty or no profit is possible, returns 0.
    """
    if not prices:
        return 0
    max_profit=0
    for i in range(1,len(prices)):
        if prices[i]>prices[i-1]:
            profit=prices[i]-prices[i-1]
            max_profit+=profit
    return max_profit            

if __name__ == "__main__":
    # Test cases: (input, expected_output)
    test_cases = [
        ([7, 1, 5, 3, 6, 4], 7),  # Buy at 1, sell at 5 (+4), buy at 3, sell at 6 (+3)
        ([7, 6, 4, 3, 1], 0),     # No profit possible
        ([1, 2, 3, 4, 5], 4),     # Buy at 1, sell at 5 (+4)
        ([5, 4, 3, 2, 1], 0),     # No profit possible
        ([3, 3, 5, 0, 0, 3, 1, 4], 8),  # Multiple transactions
        ([], 0),                  # Empty list
    ]

    for i, (input_data, expected) in enumerate(test_cases):
        result = max_profit_sell_multiple_times(input_data)
        assert result == expected, f"Test case {i + 1} failed: expected {expected}, got {result}"
    print("All test cases passed!")

'''
Time Complexity: O(n), where n is the number of prices (single pass through the list)
Space Complexity: O(1), only a constant amount of extra space is used
'''