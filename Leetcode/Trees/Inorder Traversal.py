# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right

#Iterative Solution
class Solution:
    def inorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        stack=[]
        currNode=root
        while True:
            if currNode is not None:
                stack.append(currNode)
                currNode=currNode.left
            elif stack:
                currNode=stack.pop()
                res.append(currNode.val)
                currNode=currNode.right
            else:
                return res


#With Recursion
class Solution:
    def inorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        def traverse(currNode):
            if currNode.left is not None:
                traverse(currNode.left)
            res.append(currNode.val)
            if currNode.right is not None:
                traverse(currNode.right)
        if root is not None:
            traverse(root)
        return res