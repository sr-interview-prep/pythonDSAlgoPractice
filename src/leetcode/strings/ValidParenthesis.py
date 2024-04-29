"""
Given a string s containing just the characters '(', ')', '{', '}', '[' and ']', determine if the input string is valid.

An input string is valid if:

Open brackets must be closed by the same type of brackets.
Open brackets must be closed in the correct order.


Example 1:

Input: s = "()"
Output: true
Example 2:

Input: s = "()[]{}"
Output: true
Example 3:

Input: s = "(]"
Output: false
"""


class ValidParenthesis:
    @staticmethod
    def execute(s: str) -> bool:
        dict_object = {'(': ')', '{': '}', '[': ']'}
        stack = []
        for i in s:
            if i in dict_object:
                stack.append(i)
            elif stack == [] or dict_object[stack.pop()] != i:
                return False
        if not stack:
            return True
        else:
            return False
