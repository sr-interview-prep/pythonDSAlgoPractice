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

# Longest Palindromic Substring - Expand Around Center approach

def longestPalindrome(s: str) -> str:
    if not s:
        return ""

    # These track the start and end indices of the longest palindrome found
    start, end = 0, 0

    def expand(left: int, right: int) -> tuple:
        """
        Expands outwards from the given left and right indices as long as the substring is a palindrome.
        Returns the start and end indices of the palindrome found.
        - For odd-length palindromes, left == right (centered at a character)
        - For even-length palindromes, right = left + 1 (centered between characters)
        """
        while left >= 0 and right < len(s) and s[left] == s[right]:
            left -= 1
            right += 1
        # After the loop, left and right are one step beyond the palindrome bounds
        # So the actual palindrome is s[left+1:right]
        return left + 1, right - 1 #Undo the above iteration as while loop failed palindrome there

    for i in range(len(s)):
        # Try to expand around a single character (odd-length palindrome)
        l1, r1 = expand(i, i)
        # Try to expand around a pair of characters (even-length palindrome)
        l2, r2 = expand(i, i + 1)

        # Update the longest palindrome if a longer one is found
        if r1 - l1 > end - start:
            start, end = l1, r1
        if r2 - l2 > end - start:
            start, end = l2, r2

    # Return the longest palindromic substring
    return s[start:end + 1] # +1 as it needs to include the end index value too

# Example usage and test cases
print(longestPalindrome("babad"))  # Output: "bab" or "aba"
print(longestPalindrome("cbbd"))   # Output: "bb"

# Additional test cases
assert longestPalindrome("") == ""  # Empty string
assert longestPalindrome("a") == "a"  # Single character
assert longestPalindrome("ac") == "a" or longestPalindrome("ac") == "c"  # Two different characters
assert longestPalindrome("racecar") == "racecar"  # Whole string is palindrome
assert longestPalindrome("forgeeksskeegfor") == "geeksskeeg"  # Even length palindrome in middle
assert longestPalindrome("abccba") == "abccba"  # Even length, whole string
assert longestPalindrome("abcda") == "a" or longestPalindrome("abcda") == "b" or longestPalindrome("abcda") == "c" or longestPalindrome("abcda") == "d"  # No palindrome longer than 1
print('All tests passed')
'''
Time Complexity:
- O(n^2), where n is the length of the input string.
  For each character (n), we expand around center up to n times in the worst case.

Space Complexity:
- O(1), only constant extra space is used (no extra data structures proportional to input size).
'''