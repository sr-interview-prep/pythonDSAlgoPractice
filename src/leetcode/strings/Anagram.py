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


class Anagram:
    def __init__(self, base_str: str, test_str: str):
        self.base_str = test_str
        self.test_str = base_str

    def is_anagram(self) -> bool:
        if len(self.base_str) != len(self.test_str):
            return False
        frequency_s = {}
        for i in self.base_str:
            if i not in frequency_s:
                frequency_s[i] = 1
            else:
                frequency_s[i] += 1
        frequency_t = {}
        for i in self.test_str:
            if i not in frequency_t:
                frequency_t[i] = 1
            else:
                frequency_t[i] += 1

        for i in frequency_s:
            if frequency_s.get(i) != frequency_t.get(i):
                return False

        return True
