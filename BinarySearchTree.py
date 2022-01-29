# Lesser value child nodes on left and greater value child nodes are on the right side
'''Big O
no. of nodes in a Binary Search tree are 2^n-1 where n is the no. of levels
O(log(n)) to search any element within the tree
'''

class Node:
    def __init__(self, value):
        self.value=value
        self.left=None
        self.right=None
class BinarySearchTree:
    def __init__(self):
        self.root=None
    def insert(self,value):
        new_node=Node(value)
        if self.root == None:
            self.root=new_node
            return True
        temp=self.root
        while(True):
            if new_node.value==temp.value:
                return False
            if new_node.value<temp.value:
                if temp.left is None:
                    temp.left=new_node
                    return True
                temp=temp.left
            elif new_node.value>temp.value:
                if temp.right is None:
                    temp.right=new_node
                    return True
                temp=temp.right
    def contains(self, value):
        if self.root is None:
            return False
        temp=self.root
        while(True):
            if value==temp.value:
                return True
            if value< temp.value:
                if temp.left is not None:
                    temp=temp.left
                else:
                    return False
            elif value>temp.value:
                if temp.right is not None:
                    temp=temp.right
                else:
                    return False
    def min_value_node(self, current_node):
        while current_node.left is not None:
            current_node=current_node.left
        return current_node



bst=BinarySearchTree()
bst.insert(47)
bst.insert(21)
bst.insert(76)
bst.insert(18)
bst.insert(27)
bst.insert(52)
bst.insert(82)
print(bst.root.value)
print(bst.root.left.value)
print(bst.root.right.value)

print(bst.contains(27))
print(bst.contains(17))
print(bst.min_value_node(bst.root).value)
print(bst.min_value_node(bst.root.right).value)