from src.leetcode.strings.LongestNonRepeatingSubstring import LongestNonRepeatingSubstring


def test_longest_non_repeating_substring():
    test_string = 'abcabcbb'
    solution = LongestNonRepeatingSubstring(test_string)
    length_longest_substring = solution.length_of_longest_substring()
    assert length_longest_substring == 3
    longest_substring = solution.get_longest_substring()
    assert longest_substring == 'abc'

    test_string = 'bbbbb'
    solution = LongestNonRepeatingSubstring(test_string)
    length_longest_substring = solution.length_of_longest_substring()
    assert length_longest_substring == 1
    longest_substring = solution.get_longest_substring()
    assert longest_substring == 'b'

    test_string = 'pwwkew'
    solution = LongestNonRepeatingSubstring(test_string)
    length_longest_substring = solution.length_of_longest_substring()
    assert length_longest_substring == 3
    longest_substring = solution.get_longest_substring()
    assert longest_substring == 'wke'
