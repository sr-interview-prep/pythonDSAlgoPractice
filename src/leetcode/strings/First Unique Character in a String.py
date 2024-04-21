"""Given a string s, find the first non-repeating character in it and return its index. If it does not exist, return -1.

 

Example 1:

Input: s = "leetcode"
Output: 0
Example 2:

Input: s = "loveleetcode"
Output: 2
Example 3:

Input: s = "aabb"
Output: -1
"""


class Solution:
    def firstUniqChar(self, s: str) -> int:
        general_characters = set()
        repeated_characters = set()
        for i in s:
            if i not in general_characters:
                general_characters.add(i)
            else:
                repeated_characters.add(i)
        for i in range(len(s)):
            if s[i] not in repeated_characters:
                return i
        return -1

    def alternate_first_unique_char(sef, s: str) -> int:
        frequency = {}
        for i in s:
            if i not in frequency:
                frequency[i] = 1
            else:
                frequency[i] += 1
        for i in range(len(s)):
            if frequency[s[i]] == 1:
                return i
        return -1
