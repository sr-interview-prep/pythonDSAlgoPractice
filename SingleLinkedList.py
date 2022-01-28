# Write a linked list with the below functions
'''
append, prepend, pop, pop_first, get, set_value, insert, remove, 
print_list, reverse
'''

class Node:
    def __init__(self, value):
        self.value=value
        self.next=None
class LinkedList:
    def __init__(self,value):
        new_node=Node(value)
        self.head=new_node
        self.tail=new_node
        self.length=1
    def append(self, value):
        new_node=Node(value)
        if self.length==0:
            self.head=new_node
            self.tail=new_node
        else:
            self.tail.next=new_node
            self.tail=new_node
        self.length+=1
        return True
    def prepend(self, value):
        new_node=Node(value)
        if self.length==0:
            self.head=new_node
            self.tail=new_node
            return True
        new_node.next=self.head
        self.head=new_node
        self.length+=1
        return True
    def pop(self):
        if self.length==0:
            return False
        temp=self.head
        while(temp.next):
            prev=temp
            temp=temp.next
        prev.next=None
        self.tail=prev
        self.length-=1
        if self.length==0:
            self.head=None
            self.tail=None
        return temp
    def pop_first(self):
        if self.length==0:
            return False
        elif self.head.next is None:
            self.head=None
            self.tail=None
        else:
            temp=self.head
            self.head=temp.next
            temp.next=None
        self.length-=1
        return temp
    def get(self, index):
        if index<0 or index>=self.length:
            return False
        temp=self.head
        for _ in range(index):
            temp=temp.next
        return temp
    def set_value(self, index, value):
        temp=self.get(index)
        if (temp):#in case the index value doesn't exist
            temp.value=value
            return True
        return False
    def insert(self, index, value):
        if index<0 or index>=self.length:
            return False
        if index==0:
            return self.prepend(value)
        if index==self.length:
            return self.append(value)
        new_node=Node(value)
        prev=self.get(index-1)
        temp=prev.next
        prev.next=new_node
        new_node.next=temp
        self.length+=1
        return True
    def remove(self, index):
        if index<0 or index>=self.length:
            return False
        if index==0:
            return self.pop_first()
        if index==self.length:
            return self.pop()
        prev=self.get(index-1)
        temp=prev.next
        prev.next=temp.next
        temp.next=None
        self.length-=1
        return temp
    def print_list(self):
        temp=self.head
        for _ in range(self.length):
            print(temp)
            temp=temp.next
    def reverse(self):
        # reverse the head and tail
        temp=self.head
        self.head=self.tail
        self.tail=temp
        # define before and after values
        after=temp.next
        before=None
        # loop through the swapping
        for _ in range(self.length):
            after=temp.next
            temp.next=before
            before=temp
            temp=after



    


        

