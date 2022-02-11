# Write a Single linked list with the below functions
'''
append, prepend, pop, pop_first, get, set_value, insert, remove, 
print_list, reverse
'''

class Node:
    def __init__(self,value):
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
        else:
            new_node.next=self.head
            self.head=new_node
        self.length+=1
        return True
    def pop(self):
        if self.length==0:
            return False
        temp=self.head
        if self.length==1:
            self.head=None
            self.tail=None
        else:  
            while(temp.next):
                before=temp
                temp=temp.next
            before.next=None
            self.tail=before
        self.length-=1
        return temp
    def pop_first(self):
        if self.length==0:
            return False
        temp=self.head
        if self.length==1:
            self.head=None
            self.tail=None
        else:   
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
        if temp:
            temp.value=value
            return True
        return False
    def insert(self, index, value):
        if index<0 or index>=self.length:
            return False
        if index==0:
            return self.prepend(value)
        if index==self.length-1:
            return self.append(value)
        new_node=Node(value)
        before = self.get(index-1)
        after=before.next
        before.next=new_node
        new_node.next=after
        self.length+=1
        return True
    def remove(self, index):
        if index<0 or index>=self.length:
            return False
        if index==0:
            return self.pop_first()
        if index==self.length-1:
            return self.pop()
        if self.length==1:
            temp=self.head
            self.head=None
            self.tail=None
            self.length==0
            return temp
        before=self.get(index-1)
        temp=before.next
        after=temp.next
        before.next=after
        temp.next=None
        self.length-=1
        return temp
    def print_list(self):
        temp=self.head
        for _ in range(self.length):
            print(temp.value)
            temp=temp.next
    def reverse(self):
        #reverse head and tail
        temp=self.head
        self.head=self.tail
        self.tail=temp
        #define the before value
        before=None
        #swap the links over a loop and move the before and current pointers forward
        for _ in range(self.length):
            after=temp.next
            temp.next=before
            before=temp
            temp=after
        return True







        


