"""
Algo: Expand around center
single center -> starting point left=right=i
input= babad
outer_index, palindrome -> len, start
0          ,b          -> 1, 0
1          ,bab        -> 3, 0
2          ,babad      -> 5, 0

double center -> starting point left=i, right=i+1
input= cbbd
outer_index, palindrome -> len, start
0          ,cb          -> 0, 0
1          ,bb          -> 2, 1
"""


def longestPalindrome(s: str) -> str:
    if not s:
        return ""

    start = 0
    max_length = 1

    def expand_around_center(left, right):
        while left >= 0 and right < len(s) and s[left] == s[right]:
            left -= 1
            right += 1
        return right - left + 1 - 2, left + 1
        # returns length of palindrome and start index
        # right - left + 1 is the length. However, -2 coz we are adding 1 to left and right which needs to be corrected
        # left + 1 because we want the first index of the palindrome

    # Check each position as potential palindrome center
    for i in range(len(s)):
        # Odd length palindrome, single character center
        length1, start1 = expand_around_center(i, i)

        # Even length palindrome, between two characters
        length2, start2 = expand_around_center(i, i + 1)

        # Update if we found a longer palindrome
        if length1 > max_length:
            max_length = length1
            start = start1

        if length2 > max_length:
            max_length = length2
            start = start2

    return s[start:start + max_length]


# Test examples
print(longestPalindrome("babad"))  # Output: "bab" or "aba"
print(longestPalindrome("cbbd"))  # Output: "bb"
