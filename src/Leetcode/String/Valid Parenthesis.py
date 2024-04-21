'''
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
'''


class Solution:
    def isValid(self, s: str) -> bool:
        dictt={'(':')','{':'}','[':']'}
        stack=[]
        for i in s:
            if i in dictt:
                stack.append(i)
            elif stack==[] or dictt[stack.pop()]!=i:
                return False
        if stack==[]:
            return True
        else:
            return False