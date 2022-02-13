'Stack is prepend and pop_first'
'push pop and print_stack'
class Node:
    def __init__(self, value):
        self.value=value
        self.next=None
class Stack:
    def __init__(self, value):
        new_node=Node(value)
        self.top=new_node
        self.bottom=new_node
        self.length=1
    def push(self, value):
        new_node=Node(value)
        if self.length==0:
            self.top=new_node
            self.bottom=new_node
        else:
            new_node.next=self.top
            self.top=new_node
        self.length+=1
        return True
    def pop(self):
        if self.length==0:
            return False
        temp=self.top
        if self.length==1:
            self.top=None
            self.bottom=None
        else:
            temp=self.top
            after=temp.next
            temp.next=None
            self.top=after
        self.length-=1
        return temp
    def print_stack(self):
        temp=self.top
        for _ in range(self.length):
            print(temp.value)
            temp=temp.next      

s=Stack(4)
s.push(3)
s.push(5)
s.pop()
s.push(6)
s.pop()
s.print_stack()

