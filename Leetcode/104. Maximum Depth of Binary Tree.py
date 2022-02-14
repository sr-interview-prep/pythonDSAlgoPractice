'''A binary tree's maximum depth is the number of nodes along the longest path from the root node down to the farthest leaf node.'''
class Solution:
    def maxDepth(self, root: Optional[TreeNode]) -> int:
        depth=0
        def dfs(root, depth):
            if not root: return depth
            return max(dfs(root.left, depth + 1), dfs(root.right, depth + 1))
        return dfs(root, 0)
            
            
        