from src.Leetcode.String.LongestNonRepeatingSubstring import Solution


def test_longest_non_repeating_substring():
    test_string = 'abcabcbb'
    solution = Solution(test_string)
    length_longest_substring = solution.length_of_longest_substring()
    assert length_longest_substring == 3
    longest_substring = solution.get_longest_substring()
    assert longest_substring == 'abc'

    test_string = 'bbbbb'
    solution = Solution(test_string)
    length_longest_substring = solution.length_of_longest_substring()
    assert length_longest_substring == 1
    longest_substring = solution.get_longest_substring()
    assert longest_substring == 'b'

    test_string = 'pwwkew'
    solution = Solution(test_string)
    length_longest_substring = solution.length_of_longest_substring()
    assert length_longest_substring == 3
    longest_substring = solution.get_longest_substring()
    assert longest_substring == 'wke'
