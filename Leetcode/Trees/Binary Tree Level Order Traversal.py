'''
Given the root of a binary tree, return the level order traversal of its nodes' values. (i.e., from left to right, level by level).

 

Example 1:


Input: root = [3,9,20,null,null,15,7]
Output: [[3],[9,20],[15,7]]
Example 2:

Input: root = [1]
Output: [[1]]
Example 3:

Input: root = []
Output: []
'''


# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def levelOrder(self, root: Optional[TreeNode]) -> List[List[int]]:
          # base case
        if not root:
            return []
        
        stack = [root]
        levels = []
        height = 0
        while len(stack):
            
            children = []
            for node in stack:
                if node.left is not None:
                    children.append(node.left)
                if node.right is not None:
                    children.append(node.right)
            
            # add stack to level
            levels.append([])
            for node in stack:
                levels[height].append(node.val)
            # prepare children
            stack = children
            height += 1
        
        return levels
            
        