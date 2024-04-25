from leetcode.arrays.ReverseString import ReverseString


def test_reverse_string():
    reverse_string = ReverseString()
    result = reverse_string.execute(s=["h", "e", "l", "l", "o"])
    assert result == ["o", "l", "l", "e", "h"]

    result = reverse_string.execute(s=["H", "a", "n", "n", "a", "h"])
    assert result == ["h", "a", "n", "n", "a", "H"]
