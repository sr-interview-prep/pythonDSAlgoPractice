"""
Given a string s, reverse the order of characters in each word within a sentence while still preserving whitespace and initial word order.



Example 1:

Input: s = "Let's take LeetCode contest"
Output: "s'teL ekat edoCteeL tsetnoc"
Example 2:

Input: s = "God Ding"
Output: "doG gniD"
"""


class ReverseWords:
    @staticmethod
    def execute(s: str) -> str:
        def reverse(s, l, r):
            while l <= r:
                temp = s[l]
                s[l] = s[r]
                s[r] = temp
                l += 1
                r -= 1

        i = 0
        s = list(s)
        for j in range(len(s)):
            if s[j] == ' ':
                reverse(s, i, j - 1)
                i = j + 1
            elif j == len(s) - 1:
                reverse(s, i, j)

        return "".join(s)
