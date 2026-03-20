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
    def execute(st):
        dic = {"(": ")", "{": "}", "[": "]"}
        stack = []
        for i in st:
            if i in dic:
                stack.append(i)
            elif stack == []:
                return False
            elif dic[stack.pop()] != i:
                return False
        if stack == []:
            return True
        else:
            return False


if __name__ == "__main__":
    assert ValidParenthesis.execute("()") == True, "Test case 1 failed"
    print("Test case 1 passed")

    assert ValidParenthesis.execute("()[]{}") == True, "Test case 2 failed"
    print("Test case 2 passed")

    assert ValidParenthesis.execute("(]") == False, "Test case 3 failed"
    print("Test case 3 passed")

    assert ValidParenthesis.execute("([)]") == False, "Test case 4 failed"
    print("Test case 4 passed")

    assert ValidParenthesis.execute("{[]}") == True, "Test case 5 failed"
    print("Test case 5 passed")
