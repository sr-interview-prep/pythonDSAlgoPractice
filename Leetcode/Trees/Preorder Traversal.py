'''
Given the root of a binary tree, return the preorder traversal of its nodes' values.

 

Example 1:


Input: root = [1,null,2,3]
Output: [1,2,3]
Example 2:

Input: root = []
Output: []
Example 3:

Input: root = [1]
Output: [1]
'''

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right


# Iterative Solution

class Solution:
    def preorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        stack=[]
        currNode=root
        while True:
            if currNode:
                res.append(currNode.val)
                stack.append(currNode)
                currNode=currNode.left
            elif stack:
                currNode=stack.pop()
                currNode=currNode.right
            else:
                return res

# Recursive Solution
class Solution:
    def preorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        def traverse(currNode):
            res.append(currNode.val)
            if currNode.left is not None:
                traverse(currNode.left)
            if currNode.right is not None:
                traverse(currNode.right)
        if root is not None:
            traverse(root)
        return res
            
        