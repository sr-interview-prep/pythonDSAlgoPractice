# Lesser value child nodes on left and greater value child nodes are on the right side
'''Big O
no. of nodes in a Binary Search tree are 2^n-1 where n is the no. of levels
O(log(n)) to search any element within the tree
'''
'insert, contains, min_value_node, BFS,dfs_pre_order,dfs_post_order,dfs_in_order'


class Node:
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None


class BinarySearchTree:
    def __init__(self):
        self.root = None

    def insert(self, value):
        new_node = Node(value)
        if self.root == None:
            self.root = new_node
            return True
        temp = self.root
        while True:
            if value == temp.value:
                return False

            if value < temp.value:
                if temp.left is None:
                    temp.left = new_node
                    return True
                temp = temp.left
            if value > temp.value:
                if temp.right is None:
                    temp.right = new_node
                    return True
                temp = temp.right

    def contains(self, value):
        if self.root is None:
            return False
        temp = self.root
        while True:
            if value == temp.value:
                return True
            if value < temp.value:
                if temp.left is None:
                    return False
                temp = temp.left
            if value > temp.value:
                if temp.right is None:
                    return False
                temp = temp.right

    def min_value_node(self, current_node):
        while current_node.left is not None:
            current_node = current_node.left
        return current_node

    def BFS(self):
        queue = [self.root]
        results = []
        while queue:
            level_length = len(queue)
            current_level = []
            for _ in range(level_length):
                current_node = queue.pop(0)
                current_level.append(current_node.value)

                if current_node.left:
                    queue.append(current_node.left)
                if current_node.right:
                    queue.append(current_node.right)
            results.append(current_level)
        return results

    # preoder - push elements very 1st time when reached in the traversal
    # inorder - push elements 2nd time when reached in the traversal
    # postorder - push elements 3rd time when reached in the traversal
    def dfs_pre_order(self):
        results = []

        def traverse(current_node):
            results.append(current_node.value)
            if current_node.left is not None:
                traverse(current_node.left)
            if current_node.right is not None:
                traverse(current_node.right)

        traverse(self.root)
        return results

    def dfs_in_order(self):
        results = []

        def traverse(current_node):
            if current_node.left is not None:
                traverse(current_node.left)
            results.append(current_node.value)
            if current_node.right is not None:
                traverse(current_node.right)

        traverse(self.root)
        return results

    def dfs_post_order(self):
        results = []

        def traverse(current_node):
            if current_node.left is not None:
                traverse(current_node.left)
            if current_node.right is not None:
                traverse(current_node.right)
            results.append(current_node.value)

        traverse(self.root)
        return results


if __name__ == "__main__":
    bst = BinarySearchTree()
    bst.insert(4)
    bst.insert(2)
    bst.insert(6)
    bst.insert(1)
    bst.insert(3)
    bst.insert(5)
    bst.insert(7)

    print(bst.BFS())

    # print(bst.root.value)
    # print(bst.root.left.value)
    # print(bst.root.right.value)

    # print(bst.contains(27))
    # print(bst.contains(17))
    # print(bst.min_value_node(bst.root).value)
    # print(bst.min_value_node(bst.root.right).value)
