class Node:
    def __init__(self, value):
        self.value=value
        self.next=None
class Stack:
    def __init__(self, value):
        new_node=Node(value)
        self.top=new_node
        self.height=1
    def print_stack(self):
        temp=self.top
        while temp:
            print(temp.value)
            temp=temp.next
        print('height of the stack is: ',self.height)
    def push(self, value):
        new_node=Node(value)
        if self.height==0:
            self.top=new_node
        else:
            new_node.next=self.top
            self.top=new_node
        self.height+=1
        return True
    def pop(self):
        if self.height==0:
            return False
        temp=self.top
        self.top=temp.next
        temp.next=None
        self.height-=1
        return temp

s=Stack(4)
s.push(3)
s.push(5)
s.pop()
s.print_stack()

