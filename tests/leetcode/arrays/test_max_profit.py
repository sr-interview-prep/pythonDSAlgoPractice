from leetcode.arrays.MaxProfit import MaxProfit


def test_max_profit():
    max_profit = MaxProfit(prices=[7, 1, 5, 3, 6, 4])
    result = max_profit.get_max_profit()
    assert result == 5
