# Write a Double linked list with the below functions
'''
append, prepend, pop, pop_first, get, set_value, insert, remove, 
print_list, reverse
'''
from telnetlib import DO
from tkinter.messagebox import NO


class Node:
    def __init__(self, value):
        self.value=value
        self.next=None
        self.prev=None
class DoubleLinkedList:
    def __init__(self, value):
        new_node=Node(value)
        self.head=new_node
        self.tail=new_node
        self.length=1
    def print_list(self):
        temp=self.head
        while (temp):
            print(temp.value)
            temp=temp.next
        print('lengh of the DLL is ', self.length)
    def append(self,value):
        new_node=Node(value)
        if self.head is None:
            self.head=new_node
            self.tail=new_node
        else:
            new_node.prev=self.tail
            self.tail.next=new_node
            self.tail=new_node
        self.length+=1
        return True
    def prepend(self, value):
        new_node=Node(value)
        if self.head is None:
            self.head=new_node
            self.tail=new_node
        else:
            new_node.next=self.head
            self.head.prev=new_node
            self.head=new_node
        self.length+=1
        return True
    def pop(self):
        if self.length==0:
            return False
        temp=self.tail
        before=temp.prev
        before.next=None
        self.tail=before 
        temp.prev=None
        self.length-=1
        if self.length==0:
            self.head=None
            self.tail=None
        return temp   
    def pop_first(self):
        if self.length==0:
            return False
        temp=self.head
        if self.length==0:
            self.head=None
            self.tail=None
        else:
            after=temp.next
            after.prev=None
            self.head=after
            temp.next=None
        self.length-=1
        return temp
    def get(self, index):
        if index<0 or index>self.length:
            return False
        temp=self.head
        for _ in range(index):
            temp=temp.next
        return temp
    def set(self, index, value):
        temp=self.get(index)
        if temp:
            temp.value=value
            return True
        return False
    def insert(self, index, value):
        if index<0 or index>self.length:
            return False
        if index == 0:
            return self.prepend(value)
        if index == self.length:
            return self.append(value)
        new_node=Node(value)
        before = self.get(index-1)
        after=before.next
        new_node.next=after
        new_node.prev=before
        before.next=new_node
        after.prev=new_node
        self.length+=1
        return True
    def remove(self, index):
        if index<0 or index>self.length:
            return False
        if index == 0:
            return self.pop_first()
        if index == self.length:
            return self.pop()
        before=self.get(index-1)
        temp=before.next
        after=temp.next
        before.next=after
        after.prev=before
        temp.next=None
        temp.prev=None
        self.length-=1
        return temp
DLL=DoubleLinkedList(1)
DLL.append(3)
DLL.append(4)
DLL.prepend(5)
DLL.remove(4)
DLL.print_list()





    


        




        


    