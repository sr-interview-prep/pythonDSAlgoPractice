# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right

#iterative Approach
class Solution:
    def postorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        stack=[]
        currNode=root
        while True:
            if currNode is not None:
                stack.append(currNode)
                stack.append(currNode)
                currNode=currNode.left
            elif stack:
                currNode=stack.pop()
                if len(stack)>0 and stack[-1]==currNode:
                    currNode=currNode.right
                else:
                    res.append(currNode.val)
                    currNode=None
            else:
                return res

#recursive 
class Solution:
    def postorderTraversal(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        def traverse(currNode):
            if currNode.left is not None:
                traverse(currNode.left)
            if currNode.right is not None:
                traverse(currNode.right)
            res.append(currNode.val)
        if root is not None:
            traverse(root)
        return res