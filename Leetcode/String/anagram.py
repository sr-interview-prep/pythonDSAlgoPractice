"""
Given two strings s and t, return true if t is an anagram of s, and false otherwise.

An Anagram is a word or phrase formed by rearranging the letters of a different word or phrase, typically using all the original letters exactly once.

 

Example 1:

Input: s = "anagram", t = "nagaram"
Output: true
Example 2:

Input: s = "rat", t = "car"
Output: false
"""


class Solution:
    def isAnagram(self, s: str, t: str) -> bool:
        if len(s) != len(t):
            return False
        frequency_s = {}
        for i in s:
            if i not in frequency_s:
                frequency_s[i] = 1
            else:
                frequency_s[i] += 1
        frequency_t = {}
        for i in t:
            if i not in frequency_t:
                frequency_t[i] = 1
            else:
                frequency_t[i] += 1

        for i in frequency_s:
            if frequency_t[i] != frequency_t[i]:
                return False

        return True
